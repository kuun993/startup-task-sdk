package startuptask

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// acquireErrorStore fails the acquisition step with a non-conflict error, which
// contract §7 treats as "lock state unknown".
type acquireErrorStore struct {
	Store
	releaseCalls atomic.Int32
}

func (s *acquireErrorStore) TryAcquire(context.Context, string, string, time.Time, time.Time) (bool, error) {
	return false, errors.New("acquire boom")
}

func (s *acquireErrorStore) Release(ctx context.Context, taskID, ownerID string) error {
	s.releaseCalls.Add(1)
	return s.Store.Release(ctx, taskID, ownerID)
}

func TestAcquireErrorFailsWithoutRecordsOrRelease(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	wrapped := &acquireErrorStore{Store: store}
	c := NewCoordinator(wrapped, "pod-a")
	var handlerCalls atomic.Int32
	if err := c.RegisterHandler("noop", func(map[string]any) error {
		handlerCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	const taskID = "task/acquire-error"
	result, err := c.Run(ctx, customSpec(taskID, "v1", "noop"))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %s, want FAILED", result.Outcome)
	}
	if result.Err == nil || result.Err.Error() != "acquire boom" {
		t.Fatalf("Err = %v, want acquire boom", result.Err)
	}
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0", got)
	}
	if _, ok := versionStatus(t, db, taskID, "v1"); ok {
		t.Fatalf("a version record was written although the acquire path failed")
	}
	if got := wrapped.releaseCalls.Load(); got != 0 {
		t.Fatalf("Release called %d times, want 0 (lock state unknown)", got)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows = %d, want 0", n)
	}
}

// releaseErrorStore makes Q5 fail; the outcome must not change.
type releaseErrorStore struct {
	Store
	releaseCalls atomic.Int32
}

func (s *releaseErrorStore) Release(context.Context, string, string) error {
	s.releaseCalls.Add(1)
	return errors.New("release boom")
}

func TestReleaseFailureDoesNotChangeOutcome(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	wrapped := &releaseErrorStore{Store: store}
	c := NewCoordinator(wrapped, "pod-a")
	if err := c.RegisterHandler("noop", func(map[string]any) error { return nil }); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	const taskID = "task/release-error"
	result, err := c.Run(ctx, customSpec(taskID, "v1", "noop"))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED", result.Outcome)
	}
	if result.Err != nil {
		t.Fatalf("Err = %v, want nil", result.Err)
	}
	if got := wrapped.releaseCalls.Load(); got != 1 {
		t.Fatalf("Release called %d times, want 1", got)
	}
	if status, ok := versionStatus(t, db, taskID, "v1"); !ok || status != "success" {
		t.Fatalf("version record = (%q, %v), want (success, true)", status, ok)
	}
	// The release failed, so the lock survives until its lease expires.
	if owner, ok := lockOwner(t, db, taskID); !ok || owner != "pod-a" {
		t.Fatalf("lock = (%q, %v), want (pod-a, true)", owner, ok)
	}
}
