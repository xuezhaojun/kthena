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
	"context"
	"testing"
	"time"
)

func TestSessionBoostQueue_BasicPriorityOrdering(t *testing.T) {
	cfg := DefaultSessionBoostQueueConfig()
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	// Simulate a completed session
	q.MarkSessionCompleted("conv-123")

	now := time.Now()

	normalReq := &Request{
		ReqID:         "req-normal",
		UserID:        "user-A",
		ModelName:     "model-1",
		CorrelationID: "conv-999",
		Priority:      1.0,
		RequestTime:   now,
	}
	boostReq := &Request{
		ReqID:         "req-boosted",
		UserID:        "user-B",
		ModelName:     "model-1",
		CorrelationID: "conv-123",
		Priority:      100.0,
		RequestTime:   now.Add(time.Second),
	}

	if err := q.PushRequest(normalReq); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}
	if err := q.PushRequest(boostReq); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	first, err := q.popWhenAvailable(context.Background())
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if first.ReqID != "req-boosted" {
		t.Errorf("Expected boosted request first, got %s", first.ReqID)
	}
	if !first.SessionBoost {
		t.Error("Boosted request should have SessionBoost=true")
	}

	second, err := q.popWhenAvailable(context.Background())
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if second.ReqID != "req-normal" {
		t.Errorf("Expected normal request second, got %s", second.ReqID)
	}
	if second.SessionBoost {
		t.Error("Normal request should have SessionBoost=false")
	}
}

func TestSessionBoostQueue_FIFOWithinBoostStatus(t *testing.T) {
	cfg := DefaultSessionBoostQueueConfig()
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	now := time.Now()

	// Three non-boosted requests should come out in FIFO order
	reqs := []*Request{
		{ReqID: "req-1", UserID: "u1", ModelName: "m", RequestTime: now},
		{ReqID: "req-2", UserID: "u2", ModelName: "m", RequestTime: now.Add(time.Millisecond)},
		{ReqID: "req-3", UserID: "u3", ModelName: "m", RequestTime: now.Add(2 * time.Millisecond)},
	}
	for _, r := range reqs {
		if err := q.PushRequest(r); err != nil {
			t.Fatalf("PushRequest failed: %v", err)
		}
	}

	for i, expected := range []string{"req-1", "req-2", "req-3"} {
		got, err := q.popWhenAvailable(context.Background())
		if err != nil {
			t.Fatalf("Pop %d failed: %v", i, err)
		}
		if got.ReqID != expected {
			t.Errorf("Position %d: expected %s, got %s", i, expected, got.ReqID)
		}
	}
}

func TestSessionBoostQueue_BoostExpires(t *testing.T) {
	cfg := SessionBoostQueueConfig{
		SessionBoostTTL:          50 * time.Millisecond,
		SessionBoostGracePeriod:  0,
		BackpressurePollInterval: 100 * time.Millisecond,
		InflightPerPod:           1,
	}
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	q.MarkSessionCompleted("conv-123")
	time.Sleep(60 * time.Millisecond)

	now := time.Now()
	req := &Request{
		ReqID:         "req-1",
		UserID:        "user-A",
		ModelName:     "model-1",
		CorrelationID: "conv-123",
		RequestTime:   now,
	}
	if err := q.PushRequest(req); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	popped, err := q.popWhenAvailable(context.Background())
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if popped.SessionBoost {
		t.Error("Request should not have SessionBoost after TTL expired")
	}
}

func TestSessionBoostQueue_MultipleSessions(t *testing.T) {
	cfg := DefaultSessionBoostQueueConfig()
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	q.MarkSessionCompleted("conv-A")
	q.MarkSessionCompleted("conv-B")

	now := time.Now()
	requests := []*Request{
		{ReqID: "normal-1", UserID: "u1", ModelName: "m", CorrelationID: "conv-X", RequestTime: now},
		{ReqID: "boost-A", UserID: "u2", ModelName: "m", CorrelationID: "conv-A", RequestTime: now.Add(time.Millisecond)},
		{ReqID: "boost-B", UserID: "u3", ModelName: "m", CorrelationID: "conv-B", RequestTime: now.Add(2 * time.Millisecond)},
		{ReqID: "normal-2", UserID: "u4", ModelName: "m", CorrelationID: "", RequestTime: now.Add(3 * time.Millisecond)},
	}

	for _, r := range requests {
		if err := q.PushRequest(r); err != nil {
			t.Fatalf("PushRequest failed: %v", err)
		}
	}

	first, _ := q.popWhenAvailable(context.Background())
	second, _ := q.popWhenAvailable(context.Background())
	third, _ := q.popWhenAvailable(context.Background())
	fourth, _ := q.popWhenAvailable(context.Background())

	// Both boosted should come before normal requests
	if !first.SessionBoost || !second.SessionBoost {
		t.Errorf("First two should be boosted: first=%v second=%v", first.SessionBoost, second.SessionBoost)
	}
	if third.SessionBoost || fourth.SessionBoost {
		t.Errorf("Last two should not be boosted: third=%v fourth=%v", third.SessionBoost, fourth.SessionBoost)
	}

	// Among boosted: FIFO order (boost-A arrived before boost-B)
	if first.ReqID != "boost-A" {
		t.Errorf("Expected boost-A first (earlier arrival), got %s", first.ReqID)
	}
	if second.ReqID != "boost-B" {
		t.Errorf("Expected boost-B second, got %s", second.ReqID)
	}

	// Among normal: FIFO order
	if third.ReqID != "normal-1" {
		t.Errorf("Expected normal-1 third, got %s", third.ReqID)
	}
	if fourth.ReqID != "normal-2" {
		t.Errorf("Expected normal-2 fourth, got %s", fourth.ReqID)
	}
}

func TestSessionBoostQueue_BackpressureMode(t *testing.T) {
	backendHasCapacity := true
	checker := func() bool { return backendHasCapacity }

	cfg := SessionBoostQueueConfig{
		SessionBoostTTL:          5 * time.Second,
		SessionBoostGracePeriod:  0,
		BackpressurePollInterval: 10 * time.Millisecond,
		InflightPerPod:           1,
	}
	q := NewSessionBoostQueue(nil, cfg, checker)
	q.SetPodCounter(func() int { return 2 }) // 2 pods => max 2 inflight
	defer q.Close()

	now := time.Now()
	req1 := &Request{ReqID: "req-1", UserID: "u1", ModelName: "m", RequestTime: now, NotifyChan: make(chan struct{})}
	req2 := &Request{ReqID: "req-2", UserID: "u2", ModelName: "m", RequestTime: now.Add(time.Millisecond), NotifyChan: make(chan struct{})}
	req3 := &Request{ReqID: "req-3", UserID: "u3", ModelName: "m", RequestTime: now.Add(2 * time.Millisecond), NotifyChan: make(chan struct{})}

	for _, r := range []*Request{req1, req2, req3} {
		if err := q.PushRequest(r); err != nil {
			t.Fatalf("PushRequest failed: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Run(ctx)

	// First 2 should be dequeued (inflight limit = 2)
	select {
	case <-req1.NotifyChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout: req-1 should be dequeued")
	}
	select {
	case <-req2.NotifyChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout: req-2 should be dequeued")
	}

	// Third should be blocked
	select {
	case <-req3.NotifyChan:
		t.Fatal("req-3 should be blocked by inflight limit")
	case <-time.After(100 * time.Millisecond):
	}

	if q.GetInflightCount() != 2 {
		t.Errorf("Expected inflight=2, got %d", q.GetInflightCount())
	}

	// Release one -> unblocks req3
	req1.Release()
	select {
	case <-req3.NotifyChan:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout: req-3 should be dequeued after Release()")
	}
}

func TestSessionBoostQueue_GracePeriod_BoostedArrives(t *testing.T) {
	checker := func() bool { return true }

	cfg := SessionBoostQueueConfig{
		SessionBoostTTL:          5 * time.Second,
		SessionBoostGracePeriod:  200 * time.Millisecond,
		BackpressurePollInterval: 50 * time.Millisecond,
		InflightPerPod:           1,
	}
	q := NewSessionBoostQueue(nil, cfg, checker)
	q.SetPodCounter(func() int { return 1 })
	defer q.Close()

	now := time.Now()
	req1 := &Request{
		ReqID:         "req-1",
		UserID:        "user-A",
		ModelName:     "model-1",
		CorrelationID: "session-1",
		RequestTime:   now,
		NotifyChan:    make(chan struct{}),
	}
	if err := q.PushRequest(req1); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go q.Run(ctx)

	select {
	case <-req1.NotifyChan:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout: req-1 should be dequeued")
	}

	// Mark session completed and push a non-boosted request
	q.MarkSessionCompleted("session-1")
	nonBoosted := &Request{
		ReqID:         "req-non-boosted",
		UserID:        "user-B",
		ModelName:     "model-1",
		CorrelationID: "other-session",
		RequestTime:   now.Add(time.Millisecond),
		NotifyChan:    make(chan struct{}),
	}
	if err := q.PushRequest(nonBoosted); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	// Release req1 -> triggers grace period
	req1.Release()

	// During grace, push a session-boosted follow-up
	time.Sleep(20 * time.Millisecond)
	boostedFollowUp := &Request{
		ReqID:         "req-boosted-followup",
		UserID:        "user-A",
		ModelName:     "model-1",
		CorrelationID: "session-1",
		RequestTime:   now.Add(2 * time.Millisecond),
		NotifyChan:    make(chan struct{}),
	}
	if err := q.PushRequest(boostedFollowUp); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	// Boosted follow-up should be dequeued first
	select {
	case <-boostedFollowUp.NotifyChan:
		// Success
	case <-nonBoosted.NotifyChan:
		t.Fatal("Non-boosted request should not be dequeued before the session-boosted follow-up")
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Timeout: boosted follow-up should be dequeued within grace period")
	}
}

func TestSessionBoostQueue_GracePeriod_NoBoostArrives(t *testing.T) {
	checker := func() bool { return true }

	cfg := SessionBoostQueueConfig{
		SessionBoostTTL:          5 * time.Second,
		SessionBoostGracePeriod:  100 * time.Millisecond,
		BackpressurePollInterval: 50 * time.Millisecond,
		InflightPerPod:           1,
	}
	q := NewSessionBoostQueue(nil, cfg, checker)
	q.SetPodCounter(func() int { return 1 })
	defer q.Close()

	now := time.Now()
	req1 := &Request{
		ReqID:       "req-1",
		UserID:      "user-A",
		ModelName:   "model-1",
		RequestTime: now,
		NotifyChan:  make(chan struct{}),
	}
	if err := q.PushRequest(req1); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Run(ctx)

	select {
	case <-req1.NotifyChan:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout: req-1 should be dequeued")
	}

	normalReq := &Request{
		ReqID:       "req-normal",
		UserID:      "user-B",
		ModelName:   "model-1",
		RequestTime: now.Add(time.Millisecond),
		NotifyChan:  make(chan struct{}),
	}
	if err := q.PushRequest(normalReq); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	start := time.Now()
	req1.Release()

	select {
	case <-normalReq.NotifyChan:
		elapsed := time.Since(start)
		if elapsed < 80*time.Millisecond {
			t.Errorf("Dequeue happened too early (before grace period): %v", elapsed)
		}
		if elapsed > 500*time.Millisecond {
			t.Errorf("Dequeue took too long after grace period: %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout: normal request should be dequeued after grace period expires")
	}
}

func TestSessionBoostQueue_DirectMode(t *testing.T) {
	// Without a backend checker, the queue should dequeue immediately
	cfg := DefaultSessionBoostQueueConfig()
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	q.MarkSessionCompleted("session-1")

	now := time.Now()
	normalReq := &Request{
		ReqID:         "req-normal",
		UserID:        "user-A",
		ModelName:     "model-1",
		CorrelationID: "other",
		RequestTime:   now,
		NotifyChan:    make(chan struct{}),
	}
	boostReq := &Request{
		ReqID:         "req-boosted",
		UserID:        "user-B",
		ModelName:     "model-1",
		CorrelationID: "session-1",
		RequestTime:   now.Add(time.Millisecond),
		NotifyChan:    make(chan struct{}),
	}

	if err := q.PushRequest(normalReq); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}
	if err := q.PushRequest(boostReq); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Run(ctx)

	// In direct mode, boosted should be dequeued first since it has higher heap priority
	select {
	case <-boostReq.NotifyChan:
		// Good: boosted was first
	case <-normalReq.NotifyChan:
		t.Error("Expected boosted request to be dequeued first in direct mode")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for first dequeue")
	}

	// Then normal
	select {
	case <-normalReq.NotifyChan:
		// Good
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for second dequeue")
	}
}

func TestSessionBoostQueue_CancelledRequestsSkipped(t *testing.T) {
	cfg := DefaultSessionBoostQueueConfig()
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	now := time.Now()
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	cancelledReq := &Request{
		ReqID:       "req-cancelled",
		UserID:      "user-A",
		ModelName:   "model-1",
		RequestTime: now,
		CancelCh:    cancelCtx.Done(),
	}
	normalReq := &Request{
		ReqID:       "req-normal",
		UserID:      "user-B",
		ModelName:   "model-1",
		RequestTime: now.Add(time.Millisecond),
	}

	if err := q.PushRequest(cancelledReq); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}
	if err := q.PushRequest(normalReq); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	popped, err := q.popWhenAvailable(context.Background())
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if popped.ReqID != "req-normal" {
		t.Errorf("Expected normal request (cancelled should be skipped), got %s", popped.ReqID)
	}
}

func TestSessionBoostQueue_EmptyCorrelationID(t *testing.T) {
	cfg := DefaultSessionBoostQueueConfig()
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	q.MarkSessionCompleted("conv-123")

	now := time.Now()
	req := &Request{
		ReqID:         "req-1",
		UserID:        "user-A",
		ModelName:     "model-1",
		CorrelationID: "", // No correlation ID
		RequestTime:   now,
	}

	if err := q.PushRequest(req); err != nil {
		t.Fatalf("PushRequest failed: %v", err)
	}

	popped, err := q.popWhenAvailable(context.Background())
	if err != nil {
		t.Fatalf("Pop failed: %v", err)
	}
	if popped.SessionBoost {
		t.Error("Request without CorrelationID should not have SessionBoost")
	}
}

func TestSessionBoostQueue_Len(t *testing.T) {
	cfg := DefaultSessionBoostQueueConfig()
	q := NewSessionBoostQueue(nil, cfg)
	defer q.Close()

	if q.Len() != 0 {
		t.Errorf("Expected empty queue, got len=%d", q.Len())
	}

	now := time.Now()
	for i := 0; i < 5; i++ {
		req := &Request{
			ReqID:       "req-" + string(rune('0'+i)),
			UserID:      "user",
			ModelName:   "model",
			RequestTime: now.Add(time.Duration(i) * time.Millisecond),
		}
		if err := q.PushRequest(req); err != nil {
			t.Fatalf("PushRequest failed: %v", err)
		}
	}

	if q.Len() != 5 {
		t.Errorf("Expected len=5, got %d", q.Len())
	}
}
