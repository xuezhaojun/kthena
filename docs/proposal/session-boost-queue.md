---
title: Session Boost Queue for Multi-Turn Conversation Prefix Cache Optimization
authors:
- "@YaoZengzeng"
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-06-03

---

## Session Boost Queue for Multi-Turn Conversation Prefix Cache Optimization

### Summary

This proposal introduces a **Session Boost Queue** that provides session-aware priority boosting for multi-turn conversations. It allows follow-up requests in the same conversation session to be prioritized for processing, maximizing **prefix cache hit rate** on LLM inference backends (e.g., vLLM) and significantly reducing Time-to-First-Token (TTFT) for multi-turn conversations.

### Motivation

In multi-turn conversation scenarios (e.g., ChatGPT-like interactions), each follow-up message in a conversation shares a common prefix with previous messages. Modern LLM inference engines like vLLM maintain a **prefix cache** (also called KV cache) that stores previously computed key-value attention states. When a follow-up request arrives at the same backend pod shortly after the previous request completes, the prefix cache is still warm, enabling the engine to skip recomputing attention for the shared prefix — dramatically reducing TTFT (often by 50-80%).

However, without session-aware scheduling, a follow-up request from the same conversation may be queued behind unrelated requests from other users. By the time it reaches the backend, the prefix cache may have been evicted, forcing a full recomputation.

The Session Boost Queue addresses this problem by promoting follow-up requests from recently completed sessions to the head of the request queue.

Rather than introducing a separate queue type, session boost is implemented as a **mode of the shared request priority queue** (`RequestPriorityQueue`) that also powers [fairness scheduling](../kthena/docs/user-guide/fairness-scheduling.md). The two modes reuse the same heap, enqueue/dequeue, cancellation, backpressure, and shutdown machinery; they differ only in how request priority is computed:

- **Fairness mode** (default): orders requests by per-user recent token usage.
- **Session-boost mode** (`ENABLE_SESSION_BOOST=true`): disables per-user fairness ordering and instead promotes requests whose session completed recently.

Enabling session boost therefore reconfigures the same per-model priority queue into a session-aware queue; the two modes are mutually exclusive.

#### Goals

1. **Simple activation**: Session boost can be enabled via `ENABLE_SESSION_BOOST=true` on top of fairness scheduling (`ENABLE_FAIRNESS_SCHEDULING=true`). Because session boost is a mode of the fairness queue, fairness scheduling is a prerequisite; if session boost is requested without it, the router logs a warning and ignores it.
2. **Configurable session identification**: Users can configure which HTTP header identifies conversation sessions via `SESSION_BOOST_HEADER` (e.g., `X-Session-ID`).
3. **Prefix cache optimization**: Prioritize follow-up requests from recently completed sessions to maximize warm cache hits.
4. **Grace period**: After a request completes, briefly hold the dequeue slot for a potential follow-up from the same session before dispatching unrelated requests.
5. **Backpressure-aware**: Respect backend pod capacity to avoid flooding, using two-level admission control (inflight limit + backend metrics).

#### Non-Goals

1. **Cross-router session state coordination**: Each router instance maintains independent session state.
2. **Guaranteed pod affinity**: Session boost only prioritizes queue ordering; it does not guarantee the request routes to the same pod (that's the domain of session sticky).
3. **Persistent session state**: Session tracking state does not survive router restarts.
4. **Replacing session sticky**: Session boost complements but does not replace pod-level session affinity for stateful inference.

### Proposal

#### Architecture

Session boost is a mode of the shared per-model request priority queue that sits in the request processing pipeline between the router's HTTP handler and the backend load balancer. When `ENABLE_SESSION_BOOST=true`, the same `RequestPriorityQueue` that implements fairness scheduling is constructed in session-boost mode instead of user-fairness mode.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Request Flow                                   │
│                                                                         │
│  HTTP Request                                                           │
│       │                                                                 │
│       ▼                                                                 │
│  ┌───────────┐    ┌──────────────────┐    ┌──────────────────────────┐  │
│  │  Router   │───▶│ RequestPriority  │───▶│  Backend Load Balancer   │  │
│  │  Handler  │    │ Queue            │    │  (scheduler + plugins)   │  │
│  │           │    │ (session-boost   │    │                          │  │
│  │           │    │  mode)           │    │                          │  │
│  └───────────┘    └──────────────────┘    └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Core Mechanism: Session Tracking and Priority Boosting

```
┌──────────────────────────────────────────────────────────┐
│          Session Boost Queue Internals                   │
│                                                          │
│  ┌──────────────────┐     ┌─────────────────────────┐    │
│  │ SessionTracker   │◀────│ MarkSessionCompleted()  │    │
│  │                  │     │ (after response sent)   │    │
│  │ map[corrID]time  │     └─────────────────────────┘    │
│  │ TTL: 60s default │                                    │
│  └────────┬─────────┘                                    │
│            │                                             │
│            │ HasRecentCompletion(corrID)?                │
│            ▼                                             │
│  ┌──────────────────┐                                    │
│  │  PushRequest()   │                                    │
│  │                  │                                    │
│  │  if recent ───▶ SessionBoost = true                   │
│  │  else      ───▶ SessionBoost = false                  │
│  └────────┬─────────┘                                    │
│            │                                             │
│            ▼                                             │
│  ┌──────────────────────────────────────┐                │
│  │       Priority Heap                  │                │
│  │                                      │                │
│  │  Ordering:                           │                │
│  │  1. SessionBoost=true > false        │                │
│  │  2. Within same boost: FIFO          │                │
│  │                                      │                │
│  │  [Boosted-1] [Boosted-2] [Normal-1]  │                │
│  └──────────────────────────────────────┘                │
│            │                                             │
│            ▼                                             │
│  ┌──────────────────────────────────────┐                │
│  │  Backpressure Dequeue Gate           │                │
│  │                                      │                │
│  │  Gate 1: inflight < MaxConcurrent    │                │
│  │  Gate 2: backendChecker() == true    │                │
│  │  Gate 3: grace period (optional)     │                │
│  └──────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────┘
```

#### Session Boost Lifecycle (Multi-Turn Conversation)

```
Time ──────────────────────────────────────────────────────────────────▶

User A, Session "conv-123":

  Turn 1: "Hello, tell me about Kubernetes"
  ┌──────────┐    ┌──────────┐    ┌──────────────┐    ┌────────────────┐
  │ Enqueue  │──▶│  Dequeue │───▶│ Process on   │──▶│ MarkCompleted  │
  │ (no      │    │  (normal │    │ Pod-X        │    │ ("conv-123")   │
  │  boost)  │    │   order) │    │              │    │                │
  └──────────┘    └──────────┘    └──────────────┘    └───────┬────────┘
                                                              │
                            SessionTracker["conv-123"] = now  │
                                                              │
  Turn 2: "Can you give more details on pods?"                │
  ┌──────────┐                                                │
  │  Enqueue  │ ◀── HasRecentCompletion("conv-123") = true ───┘
  │  (BOOST   │     (within TTL)
  │   =true)  │
  └────┬─────┘
       │
       ▼ (Promoted to heap head, ahead of all non-boosted requests)
  ┌──────────┐    ┌──────────────┐
  │  Dequeue  │───▶│  Process on  │  ← Prefix cache HIT! TTFT reduced 50-80%
  │  (first!) │    │  Pod-X*      │    (*if session sticky also routes here)
  └──────────┘    └──────────────┘
```

#### Grace Period Mechanism

The grace period is a brief wait after a request completes (Release), designed to give the same session's follow-up request a chance to arrive and be prioritized:

```
Timeline:
         ┌─ req-1 completes, Release() called
         │
         │  ┌── Grace Period (default 50ms) ──┐
         │  │                                  │
         ▼  ▼                                  ▼
    ─────┼──┼──────────────────────────────────┼─────
         │  │                                  │
         │  │  Case A: Boosted request arrives │
         │  │  during grace → dequeue now      │
         │  │                                  │
         │  │  Case B: No boost arrives →      │
         │  │  dequeue normal head after grace │
         │  │                                  │
         │  │  Case C: Head already boosted →  │
         │  │  skip grace, dequeue immediately │
         │  └──────────────────────────────────┘
```

#### Configuration

| Environment Variable          | Default        | Description                                                                                                                                                                                   |
| ----------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ENABLE_FAIRNESS_SCHEDULING`  | `false`        | Enable the fairness queue. Required for session boost, which runs as a mode of this queue                                                                                                     |
| `ENABLE_SESSION_BOOST`        | `false`        | Enable session-boost mode on the fairness queue (requires `ENABLE_FAIRNESS_SCHEDULING=true`)                                                                                                  |
| `SESSION_BOOST_HEADER`        | `X-Session-ID` | HTTP header used to identify conversation sessions                                                                                                                                            |
| `SESSION_BOOST_TTL`           | `60s`          | Duration after which a session boost expires                                                                                                                                                  |
| `SESSION_BOOST_GRACE_PERIOD`  | `0`            | Wait time after release for same-session follow-up. Disabled by default; enable only when you understand the latency trade-off                                                                |
| `SESSION_BOOST_POLL_INTERVAL` | `100ms`        | Backend capacity polling interval                                                                                                                                                             |
| `FAIRNESS_MAX_CONCURRENT`     | `16`           | Reused from fairness scheduling as the global (total) inflight limit in session-boost mode. Operators size it from the estimated per-pod concurrency multiplied by the number of backend pods |

### Design Details

#### Data Structures

Session boost reuses the shared `RequestPriorityQueue` and its `FairnessQueueConfig`. The session-boost options are additional fields on that config, and session-boost state is added to the queue itself. When `SessionBoostEnabled` is set, the queue's `Less` comparison switches from per-user fairness ordering to session-boost ordering, and `Run` dispatches to the backpressure/grace-period dequeue loop instead of the QPS/semaphore loop.

```go
// FairnessQueueConfig (shared) — session-boost fields
type FairnessQueueConfig struct {
    // ... fairness fields (TokenWeight, RequestNumWeight, MaxQPS, ...) ...

    // MaxConcurrent is reused in session-boost mode as the global (total) inflight
    // limit (default 16 when unset). Operators size it from per-pod concurrency
    // multiplied by pod count.
    MaxConcurrent int

    SessionBoostEnabled      bool          // Switch from user-fairness to session-boost mode
    SessionIDHeader          string        // HTTP header for session identification
    SessionBoostTTL          time.Duration // How long a session boost is valid
    SessionBoostGracePeriod  time.Duration // Wait for same-session follow-up (default: 0, disabled)
    BackpressurePollInterval time.Duration // Backend polling frequency
}

// RequestPriorityQueue (shared) — session-boost state
type RequestPriorityQueue struct {
    heap         []*Request      // Shared heap; ordering depends on mode (see Less)
    config       FairnessQueueConfig
    tokenTracker TokenTracker    // Used for fairness-mode priority

    // Session-boost mode (active when config.SessionBoostEnabled)
    sessionBoost   bool
    sessionTracker *SessionTracker       // Tracks recently completed sessions
    backendChecker BackendWaitingChecker // Backend capacity gate
    inflightCount  atomic.Int64          // Current inflight requests
    releaseCh      chan struct{}         // Release-driven dequeue signal
}

// Session-boost ordering (RequestPriorityQueue.Less when sessionBoost == true):
// 1. SessionBoost=true comes before SessionBoost=false
// 2. Within same boost status: earlier RequestTime comes first (FIFO)
```

#### Session Identification

Sessions are identified by a configurable HTTP header (default: `X-Session-ID`, controlled by `SESSION_BOOST_HEADER` environment variable). This is a client-provided identifier that groups related requests in a multi-turn conversation:

```
POST /v1/chat/completions
X-Session-ID: conv-abc-123
X-Request-ID: req-001

{"model": "llama-3", "messages": [...]}
```

Operators can customize the header name to match their client conventions:

```bash
# Use a custom header for session identification
export SESSION_BOOST_HEADER="X-Session-ID"
```

When a request with the configured session header (e.g., `X-Session-ID: conv-abc-123`) completes successfully, the session tracker records the completion time. Any subsequent request within the TTL window that carries the same session identifier will be marked as session-boosted and promoted to the head of the queue.

#### Backpressure Control

The queue uses two-level admission control:

1. **Inflight limit**: At most `MaxConcurrent` requests can be in-flight across all backends simultaneously. `MaxConcurrent` is reused from fairness scheduling (`FAIRNESS_MAX_CONCURRENT`) as a global limit; operators size it from the estimated per-pod concurrency and pod count. This prevents flooding backends between metric scrapes.
2. **Backend metrics check**: The `BackendWaitingChecker` polls backend pod metrics (e.g., vLLM's `RequestWaitingNum`) to confirm at least one pod has capacity.

When a request completes (Release), the queue immediately attempts to dequeue the next request (release-driven dequeue) rather than waiting for the next polling tick, ensuring minimal latency between sequential requests.

### Multi-Turn Conversation Advantages

#### 1. Prefix Cache Hit Rate Improvement

In a typical multi-turn conversation, each message includes the full conversation history as context:

```
Turn 1: System prompt + User message 1           (1000 tokens)
Turn 2: System prompt + User message 1 + Response 1 + User message 2  (3000 tokens)
Turn 3: System prompt + ... + User message 3     (5000 tokens)
```

Without session boost, Turn 2 may be queued behind 10 other requests. By the time it reaches the backend, the KV cache entries for the first 1000 tokens may be evicted. With session boost, Turn 2 is prioritized immediately after Turn 1 completes, hitting the warm prefix cache and only computing attention for the new ~2000 tokens.

**Expected TTFT improvement**: For a 5000-token prompt where 3000 tokens are cached prefix, TTFT is reduced by approximately 60% (only 2000 tokens need computation vs 5000).

#### 2. Grace Period for Natural Conversation Flow

Human users typically take 1-50ms between receiving a response and submitting the next message (for automated pipelines) or the follow-up may arrive within seconds (for human users). The grace period is disabled by default (`0`) to avoid adding latency to non-boosted requests. When explicitly enabled (e.g., `50ms`), it holds the dequeue slot briefly for automated multi-turn pipelines (like RAG chains or agentic workflows) that issue follow-up requests programmatically. Only enable it if you understand the trade-off.

#### 3. No User ID Requirement for Priority

Session boost derives priority from the configured session header (default: `X-Session-ID`) rather than the user identity. Even though it runs as a mode of the fairness queue, the session-boost ordering does not depend on a user ID, so prefix cache optimization works for unauthenticated requests (which are simply enqueued without a boost until their session completes once).

#### 4. Complementary with Session Sticky

When combined with session sticky routing (which routes the same session to the same pod), session boost ensures the follow-up request is both:
- **Prioritized** (processed sooner via session boost queue)
- **Routed to the same pod** (via session sticky)

This combination maximizes prefix cache benefits: the request arrives quickly AND hits the pod where the cache is stored.
