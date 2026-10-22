package startuptask

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"
	"time"
)

// SystemClock must feed RunResult.Duration with a readable elapsed time. The wall
// clock on this host has ~1ms granularity, so the handler sleeps long enough to be
// unambiguous instead of asserting on sub-millisecond runs.
func TestSystemClockDurationReflectsElapsed(t *testing.T) {
	ctx := context.Background()
	_, store := newTestStore(t)

	c := NewCoordinator(store, "pod-clock")
	if err := c.RegisterHandler("slow", func(map[string]any) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	result, err := c.Run(ctx, customSpec("task/system-clock", "v1", "slow"))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	if result.Duration < 10*time.Millisecond {
		t.Fatalf("Duration = %v, want >= 10ms for a 20ms handler", result.Duration)
	}
	if result.Duration > 5*time.Second {
		t.Fatalf("Duration = %v, implausibly large", result.Duration)
	}
}

// fakeClock is a deterministic Clock; the coordinator stores it by interface, so
// pointer receivers are required for Advance/Set to be observed.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// The injected clock determines acquired_at / expires_at / executed_at and the
// reported duration, so all of them become exactly assertable.
func TestInjectedClockDrivesTimestampsAndDuration(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	base := time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)
	clock := newFakeClock(base)
	c := NewCoordinatorWithClock(store, "pod-a", clock)

	const taskID = "task/clock"
	var acquiredAt, expiresAt string
	if err := c.RegisterHandler("advance", func(map[string]any) error {
		if err := db.QueryRow(
			`SELECT acquired_at, expires_at FROM task_execution_lock WHERE task_id = ?`, taskID).
			Scan(&acquiredAt, &expiresAt); err != nil {
			return err
		}
		// Pretend the handler took 150ms.
		clock.Advance(150 * time.Millisecond)
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	spec := customSpec(taskID, "v1", "advance") // TimeoutSeconds = 30
	result, err := c.Run(ctx, spec)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}

	if want := formatSQLiteTime(base); acquiredAt != want {
		t.Fatalf("acquired_at = %q, want %q", acquiredAt, want)
	}
	if want := formatSQLiteTime(base.Add(30 * time.Second)); expiresAt != want {
		t.Fatalf("expires_at = %q, want %q", expiresAt, want)
	}

	var executedAt string
	if err := db.QueryRow(
		`SELECT executed_at FROM task_version_record WHERE task_id = ? AND version = ?`,
		taskID, "v1").Scan(&executedAt); err != nil {
		t.Fatalf("select executed_at: %v", err)
	}
	if want := formatSQLiteTime(base.Add(150 * time.Millisecond)); executedAt != want {
		t.Fatalf("executed_at = %q, want %q", executedAt, want)
	}

	if got := result.DurationMs(); got != 150 {
		t.Fatalf("DurationMs = %v, want exactly 150", got)
	}
}

// expires_at is compared with a strict "<": a lock expiring exactly now is still
// held, one microsecond in the past is taken over.
func TestInjectedClockExpiryBoundary(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	base := time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)

	insertLock := func(taskID, owner string, expires time.Time) {
		t.Helper()
		if _, err := db.Exec(
			`INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
			taskID, owner, formatSQLiteTime(base.Add(-time.Hour)), formatSQLiteTime(expires)); err != nil {
			t.Fatalf("insert lock: %v", err)
		}
	}

	c := NewCoordinatorWithClock(store, "new-pod", newFakeClock(base))
	if err := c.RegisterHandler("noop", func(map[string]any) error { return nil }); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	// expires_at == now => still held.
	insertLock("task/boundary-equal", "old-pod", base)
	result, _ := c.Run(ctx, customSpec("task/boundary-equal", "v1", "noop"))
	if result.Outcome != OutcomeSkippedLocked {
		t.Fatalf("expires_at == now: outcome = %s, want SKIPPED_LOCKED (err: %v)", result.Outcome, result.Err)
	}

	// expires_at == now - 1µs => takeover allowed.
	insertLock("task/boundary-past", "old-pod", base.Add(-time.Microsecond))
	result, _ = c.Run(ctx, customSpec("task/boundary-past", "v1", "noop"))
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("expires_at == now-1µs: outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
}

func TestLeaseDurationClamping(t *testing.T) {
	cases := []struct {
		seconds int
		want    time.Duration
	}{
		{seconds: 0, want: 0},
		{seconds: -5, want: 0},
		{seconds: 300, want: 300 * time.Second},
		{seconds: maxLeaseSeconds, want: time.Duration(maxLeaseSeconds) * time.Second},
		{seconds: maxLeaseSeconds + 1, want: time.Duration(maxLeaseSeconds) * time.Second},
		{seconds: math.MaxInt, want: time.Duration(maxLeaseSeconds) * time.Second},
	}
	for _, tc := range cases {
		if got := leaseDuration(tc.seconds); got != tc.want {
			t.Fatalf("leaseDuration(%d) = %v, want %v", tc.seconds, got, tc.want)
		}
		if got := leaseDuration(tc.seconds); got < 0 {
			t.Fatalf("leaseDuration(%d) overflowed to %v", tc.seconds, got)
		}
	}
}

// An extreme timeout_seconds must not overflow the lease: expires_at is clamped
// to 100 years after the injected "now".
func TestHugeTimeoutIsClampedWhenComputingExpiresAt(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	base := time.Date(2026, 10, 22, 8, 0, 0, 0, time.UTC)
	c := NewCoordinatorWithClock(store, "pod-a", newFakeClock(base))

	const taskID = "task/huge-timeout"
	var expiresAt string
	if err := c.RegisterHandler("inspect", func(map[string]any) error {
		return db.QueryRow(
			`SELECT expires_at FROM task_execution_lock WHERE task_id = ?`, taskID).Scan(&expiresAt)
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	spec := customSpec(taskID, "v1", "inspect")
	spec.TimeoutSeconds = math.MaxInt

	result, _ := c.Run(ctx, spec)
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	want := formatSQLiteTime(base.Add(3153600000 * time.Second)) // 100 * 365 days
	if expiresAt != want {
		t.Fatalf("expires_at = %q, want %q", expiresAt, want)
	}
}

// The default owner_id is computed once per process (contract §2), and an
// explicit owner is used verbatim.
func TestDefaultOwnerIDIsProcessStable(t *testing.T) {
	_, store := newTestStore(t)

	first := NewCoordinator(store, "")
	second := NewCoordinatorWithClock(store, "", newFakeClock(time.Now().UTC()))
	if first.OwnerID() != second.OwnerID() {
		t.Fatalf("default owner changed between coordinators: %q vs %q",
			first.OwnerID(), second.OwnerID())
	}

	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	owner := first.OwnerID()
	if len(owner) != len(host)+1+8 {
		t.Fatalf("owner = %q, want %q/<8 hex chars>", owner, host)
	}
	if owner[:len(host)+1] != host+"/" {
		t.Fatalf("owner = %q, want prefix %q", owner, host+"/")
	}
	for _, r := range owner[len(host)+1:] {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			t.Fatalf("owner suffix %q is not lowercase hex", owner[len(host)+1:])
		}
	}

	if got := NewCoordinator(store, "pod-explicit").OwnerID(); got != "pod-explicit" {
		t.Fatalf("explicit owner = %q, want pod-explicit", got)
	}
}
