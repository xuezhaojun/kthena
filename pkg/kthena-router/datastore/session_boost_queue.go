/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datastore

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	"github.com/volcano-sh/kthena/pkg/kthena-router/metrics"
)

// BackendWaitingChecker is a function that checks whether the backend pods
// have capacity to accept new requests. It returns true when at least one pod
// has an empty waiting queue (i.e. RequestWaitingNum == 0), meaning the backend
// can accept a new request without queuing.
type BackendWaitingChecker func() bool

// SessionTracker tracks recently completed sessions for priority boosting.
// It maps correlation IDs to their last completion time, allowing follow-up
// requests in the same session to be prioritized for prefix cache utilization.
type SessionTracker struct {
	mu       sync.RWMutex
	sessions map[string]time.Time // correlationID -> last completion time
	ttl      time.Duration
}

// NewSessionTracker creates a new session tracker with the given TTL.
func NewSessionTracker(ttl time.Duration) *SessionTracker {
	return &SessionTracker{
		sessions: make(map[string]time.Time),
		ttl:      ttl,
	}
}

// MarkCompleted records that a request from the given session has completed.
func (st *SessionTracker) MarkCompleted(correlationID string) {
	if correlationID == "" {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sessions[correlationID] = time.Now()
}

// HasRecentCompletion checks if the given correlation ID has a completion within the TTL window.
func (st *SessionTracker) HasRecentCompletion(correlationID string) bool {
	if correlationID == "" {
		return false
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	completionTime, exists := st.sessions[correlationID]
	if !exists {
		return false
	}
	return time.Since(completionTime) <= st.ttl
}

// Cleanup removes expired sessions. Should be called periodically.
func (st *SessionTracker) Cleanup() {
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	expired := 0
	for id, t := range st.sessions {
		if now.Sub(t) > st.ttl {
			delete(st.sessions, id)
			expired++
		}
	}
	if expired > 0 {
		klog.V(4).Infof("[SessionTracker] cleanup: removed %d expired sessions, remaining=%d", expired, len(st.sessions))
	}
}

// ActiveSessions returns the number of sessions currently tracked.
func (st *SessionTracker) ActiveSessions() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.sessions)
}

// SessionBoostQueueConfig holds configurable parameters for the standalone session boost queue.
type SessionBoostQueueConfig struct {
	// SessionIDHeader is the HTTP header name used to identify conversation sessions.
	// Defaults to "X-Correlation-ID".
	SessionIDHeader string

	// SessionBoostTTL is the duration after which a session boost expires.
	// Requests from the same session that arrive within this window after the
	// previous request completed will be boosted.
	SessionBoostTTL time.Duration

	// SessionBoostGracePeriod is the duration to wait after a release before dequeuing
	// the next request in backpressure mode.
	// This gives the same session time to submit a follow-up request that benefits
	// from prefix cache, rather than immediately dispatching an unrelated request.
	// If a session-boosted request arrives during this window, it is dequeued immediately.
	// Defaults to 50ms. Set to 0 to disable the grace period.
	SessionBoostGracePeriod time.Duration

	// BackpressurePollInterval controls how often the backpressure checker polls
	// backend pod waiting queue status. Defaults to 100ms.
	BackpressurePollInterval time.Duration

	// InflightPerPod is the maximum number of inflight requests allowed per backend pod.
	// The total inflight limit is InflightPerPod * podCount.
	// Defaults to 1.
	InflightPerPod int
}

// DefaultSessionBoostQueueConfig returns default configuration for the session boost queue.
func DefaultSessionBoostQueueConfig() SessionBoostQueueConfig {
	return SessionBoostQueueConfig{
		SessionIDHeader:          "X-Correlation-ID",
		SessionBoostTTL:          60 * time.Second,
		SessionBoostGracePeriod:  50 * time.Millisecond,
		BackpressurePollInterval: 100 * time.Millisecond,
		InflightPerPod:           1,
	}
}

// sessionBoostHeap implements heap.Interface for session boost priority ordering.
// Boosted requests always take priority over non-boosted ones.
// Within the same boost status, FIFO ordering is used.
type sessionBoostHeap struct {
	items []*Request
}

func (h *sessionBoostHeap) Len() int { return len(h.items) }

func (h *sessionBoostHeap) Less(i, j int) bool {
	// Session-boosted requests always take priority over non-boosted ones.
	if h.items[i].SessionBoost != h.items[j].SessionBoost {
		return h.items[i].SessionBoost
	}
	// Within same boost status, use FIFO ordering.
	return h.items[i].RequestTime.Before(h.items[j].RequestTime)
}

func (h *sessionBoostHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *sessionBoostHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*Request))
}

func (h *sessionBoostHeap) Pop() interface{} {
	n := len(h.items)
	if n == 0 {
		return nil
	}
	item := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[0 : n-1]
	return item
}

// SessionBoostQueue implements session-aware priority boosting for multi-turn
// conversations to maximize prefix cache hit rate on LLM inference backends.
type SessionBoostQueue struct {
	stopCh   chan struct{}
	notifyCh chan struct{}
	mu       sync.RWMutex
	heap     sessionBoostHeap
	metrics  *metrics.Metrics
	config   SessionBoostQueueConfig

	// Session tracking for priority boosting
	sessionTracker *SessionTracker

	// Backend capacity checking
	backendChecker BackendWaitingChecker

	// Inflight tracking for backpressure mode
	inflightCount atomic.Int64
	releaseCh     chan struct{}
	podCounter    func() int
}

// NewSessionBoostQueue creates a new standalone session boost queue.
func NewSessionBoostQueue(metricsInstance *metrics.Metrics, cfg SessionBoostQueueConfig, checker ...BackendWaitingChecker) *SessionBoostQueue {
	if metricsInstance == nil {
		metricsInstance = metrics.DefaultMetrics
	}
	q := &SessionBoostQueue{
		stopCh:         make(chan struct{}),
		notifyCh:       make(chan struct{}, 1),
		releaseCh:      make(chan struct{}, 1),
		heap:           sessionBoostHeap{items: make([]*Request, 0)},
		metrics:        metricsInstance,
		config:         cfg,
		sessionTracker: NewSessionTracker(cfg.SessionBoostTTL),
	}
	if len(checker) > 0 && checker[0] != nil {
		q.backendChecker = checker[0]
	}
	return q
}

// PushRequest adds a request to the session boost queue.
// If the request's correlation ID matches a recently completed session, it is boosted.
func (q *SessionBoostQueue) PushRequest(r *Request) error {
	q.mu.Lock()

	// Check session boost: if this request's correlation ID has a recent completion,
	// mark it as boosted so it gets priority in the queue.
	if r.CorrelationID != "" && q.sessionTracker.HasRecentCompletion(r.CorrelationID) {
		r.SessionBoost = true
	}

	heap.Push(&q.heap, r)
	queueLen := q.heap.Len()
	q.mu.Unlock()

	// Update metrics
	if q.metrics != nil {
		q.metrics.IncSessionBoostQueueSize(r.ModelName)
	}

	if r.SessionBoost {
		klog.V(4).Infof("[SessionBoostQueue] session boost: reqID=%s correlationID=%s promoted, queueLen=%d",
			r.ReqID, r.CorrelationID, queueLen)
	}

	// Signal that a new item is available
	select {
	case q.notifyCh <- struct{}{}:
	default:
	}
	return nil
}

// popWhenAvailable blocks until an item is available or the context is done.
func (q *SessionBoostQueue) popWhenAvailable(ctx context.Context) (*Request, error) {
	for {
		q.mu.Lock()
		if q.heap.Len() > 0 {
			req := heap.Pop(&q.heap).(*Request)

			// Skip cancelled requests
			if req.isCancelled() {
				if q.metrics != nil {
					q.metrics.DecSessionBoostQueueSize(req.ModelName)
					queueDuration := time.Since(req.RequestTime)
					q.metrics.RecordSessionBoostQueueDuration(req.ModelName, queueDuration)
					q.metrics.IncSessionBoostQueueCancelled(req.ModelName)
				}
				q.mu.Unlock()
				continue
			}

			queueDuration := time.Since(req.RequestTime)
			if q.metrics != nil {
				q.metrics.DecSessionBoostQueueSize(req.ModelName)
				q.metrics.RecordSessionBoostQueueDuration(req.ModelName, queueDuration)
				q.metrics.IncSessionBoostQueueDequeue(req.ModelName)
			}
			q.mu.Unlock()
			return req, nil
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.stopCh:
			return nil, errors.New("queue stopped")
		case <-q.notifyCh:
			continue
		}
	}
}

// Run starts the dequeue loop. Uses backpressure mode when a backend checker is provided,
// otherwise falls through immediately (no rate limiting — suitable for direct dispatch).
func (q *SessionBoostQueue) Run(ctx context.Context) {
	// Start session tracker cleanup goroutine
	go q.runSessionCleanup(ctx)

	if q.backendChecker != nil {
		q.runBackpressureMode(ctx)
		return
	}
	// Without backpressure checker, dequeue immediately (no rate limiting).
	q.runDirectMode(ctx)
}

// runDirectMode dequeues requests as fast as they arrive with no rate limiting.
func (q *SessionBoostQueue) runDirectMode(ctx context.Context) {
	for {
		req, err := q.popWhenAvailable(ctx)
		if err != nil {
			return
		}
		if req == nil || req.NotifyChan == nil {
			continue
		}
		if req.isCancelled() {
			continue
		}

		// Track inflight
		q.inflightCount.Add(1)
		releaseOnce := sync.Once{}
		req.Release = func() {
			releaseOnce.Do(func() {
				q.inflightCount.Add(-1)
				select {
				case q.releaseCh <- struct{}{}:
				default:
				}
				if q.metrics != nil {
					q.metrics.DecSessionBoostQueueInflight(req.ModelName)
				}
			})
		}
		if q.metrics != nil {
			q.metrics.IncSessionBoostQueueInflight(req.ModelName)
		}
		close(req.NotifyChan)
	}
}

// runSessionCleanup periodically cleans up expired sessions from the session tracker.
func (q *SessionBoostQueue) runSessionCleanup(ctx context.Context) {
	cleanupInterval := q.config.SessionBoostTTL
	if cleanupInterval < 10*time.Second {
		cleanupInterval = 10 * time.Second
	}
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case <-ticker.C:
			q.sessionTracker.Cleanup()
		}
	}
}

// runBackpressureMode dequeues requests only when backend pods have capacity.
// Uses two-level admission control:
//  1. Inflight limit: at most InflightPerPod requests per backend pod.
//  2. Backend metrics check: at least one pod reports capacity available.
//
// Session Grace Period: When SessionBoostGracePeriod > 0, a release event triggers
// a short wait before dequeuing to give the same session time to submit a follow-up
// request that can leverage prefix cache.
func (q *SessionBoostQueue) runBackpressureMode(ctx context.Context) {
	pollInterval := q.config.BackpressurePollInterval
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	klog.V(4).Infof("[SessionBoostQueue] starting backpressure dequeue loop, poll_interval=%v, gracePeriod=%v",
		pollInterval, q.config.SessionBoostGracePeriod)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case <-q.releaseCh:
			if q.config.SessionBoostGracePeriod > 0 {
				q.waitGraceAndDequeue(ctx)
			} else {
				q.tryBackpressureDequeue(ctx)
			}
		case <-ticker.C:
			q.tryBackpressureDequeue(ctx)
		}
	}
}

// isHeadSessionBoosted checks if the highest-priority request in the queue has a session boost.
func (q *SessionBoostQueue) isHeadSessionBoosted() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.heap.Len() == 0 {
		return false
	}
	return q.heap.items[0].SessionBoost
}

// waitGraceAndDequeue waits up to SessionBoostGracePeriod for a session-boosted
// request to arrive at the head of the queue.
func (q *SessionBoostQueue) waitGraceAndDequeue(ctx context.Context) {
	// Fast path: head is already session-boosted.
	if q.isHeadSessionBoosted() {
		klog.V(4).Info("[SessionBoostQueue] grace: head already boosted, skipping wait")
		q.tryBackpressureDequeue(ctx)
		return
	}

	klog.V(4).Infof("[SessionBoostQueue] grace: starting grace period %v", q.config.SessionBoostGracePeriod)
	timer := time.NewTimer(q.config.SessionBoostGracePeriod)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case <-q.notifyCh:
			if q.isHeadSessionBoosted() {
				klog.V(4).Info("[SessionBoostQueue] grace period: session-boosted request arrived, dequeuing immediately")
				q.tryBackpressureDequeue(ctx)
				return
			}
		case <-timer.C:
			q.tryBackpressureDequeue(ctx)
			return
		}
	}
}

// tryBackpressureDequeue attempts to dequeue one request if both the inflight limit
// and the backend capacity check pass.
func (q *SessionBoostQueue) tryBackpressureDequeue(ctx context.Context) {
	perPod := q.config.InflightPerPod
	if perPod <= 0 {
		perPod = 1
	}
	maxInflight := int64(perPod)
	podCount := 0
	if q.podCounter != nil {
		podCount = q.podCounter()
		if podCount > 0 {
			maxInflight = int64(podCount) * int64(perPod)
		}
	}

	currentInflight := q.inflightCount.Load()

	if currentInflight >= maxInflight {
		klog.V(4).Infof("[SessionBoostQueue] backpressure: inflight limit reached, inflight=%d maxInflight=%d pods=%d perPod=%d",
			currentInflight, maxInflight, podCount, perPod)
		return
	}

	if !q.backendChecker() {
		q.mu.RLock()
		queueLen := q.heap.Len()
		q.mu.RUnlock()
		klog.V(4).Infof("[SessionBoostQueue] backpressure: backend pods busy, holding dequeue. queueLen=%d inflight=%d pods=%d",
			queueLen, currentInflight, podCount)
		return
	}

	q.mu.RLock()
	queueLen := q.heap.Len()
	q.mu.RUnlock()
	if queueLen == 0 {
		return
	}

	req, err := q.popWhenAvailable(ctx)
	if err != nil || req == nil {
		return
	}

	q.inflightCount.Add(1)
	releaseOnce := sync.Once{}
	req.Release = func() {
		releaseOnce.Do(func() {
			q.inflightCount.Add(-1)
			select {
			case q.releaseCh <- struct{}{}:
			default:
			}
			if q.metrics != nil {
				q.metrics.DecSessionBoostQueueInflight(req.ModelName)
			}
		})
	}
	if q.metrics != nil {
		q.metrics.IncSessionBoostQueueInflight(req.ModelName)
	}

	klog.V(4).Infof("[SessionBoostQueue] backpressure dequeue: reqID=%s user=%s model=%s sessionBoost=%v inflight=%d/%d",
		req.ReqID, req.UserID, req.ModelName, req.SessionBoost, q.inflightCount.Load(), maxInflight)

	if req.NotifyChan != nil {
		close(req.NotifyChan)
	}
}

// Close stops the dequeue loop and drains pending items.
func (q *SessionBoostQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	select {
	case <-q.stopCh:
		return
	default:
		close(q.stopCh)
	}

	for q.heap.Len() > 0 {
		req := heap.Pop(&q.heap).(*Request)
		if q.metrics != nil {
			q.metrics.DecSessionBoostQueueSize(req.ModelName)
		}
	}
	klog.V(4).Info("[SessionBoostQueue] queue closed and drained")
}

// MarkSessionCompleted records that a request from the given session has completed.
func (q *SessionBoostQueue) MarkSessionCompleted(correlationID string) {
	q.sessionTracker.MarkCompleted(correlationID)
}

// GetSessionTracker returns the session tracker.
func (q *SessionBoostQueue) GetSessionTracker() *SessionTracker {
	return q.sessionTracker
}

// SetPodCounter sets the function used to determine the number of ready backend pods.
func (q *SessionBoostQueue) SetPodCounter(counter func() int) {
	q.podCounter = counter
}

// GetInflightCount returns the current number of inflight requests.
func (q *SessionBoostQueue) GetInflightCount() int64 {
	return q.inflightCount.Load()
}

// Len returns the current queue length.
func (q *SessionBoostQueue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.heap.Len()
}
