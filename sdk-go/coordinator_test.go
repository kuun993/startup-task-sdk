package startuptask

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario 1 (contract §10): empty database -> EXECUTED, success version record,
// lock released.
func TestFirstRunExecuted(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	var calls atomic.Int32
	c := NewCoordinator(store, "pod-a")
	if err := c.RegisterHandler("noop", func(map[string]any) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	spec := customSpec("task/alpha", "v1", "noop")
	result, err := c.Run(ctx, spec)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	if result.Err != nil {
		t.Fatalf("Err = %v, want nil", result.Err)
	}
	if result.TaskID != spec.TaskID || result.Version != spec.Version {
		t.Fatalf("result task/version = %s/%s, want %s/%s",
			result.TaskID, result.Version, spec.TaskID, spec.Version)
	}
	// Not asserting Duration > 0: the host wall clock has ~1ms granularity, so a
	// sub-millisecond run may legitimately measure 0. Exact durations are asserted
	// with an injected clock in TestInjectedClockDrivesTimestampsAndDuration.
	if result.Duration < 0 {
		t.Fatalf("Duration = %v, want >= 0", result.Duration)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if status, ok := versionStatus(t, db, spec.TaskID, spec.Version); !ok || status != "success" {
		t.Fatalf("version record = (%q, %v), want (success, true)", status, ok)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows = %d, want 0 (lock must be released)", n)
	}
}

// Scenario 2 (contract §10): a second run is skipped and the lock table is
// touched neither before nor after.
func TestSecondRunSkippedAlreadyDone(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	var calls atomic.Int32
	c := NewCoordinator(store, "pod-a")
	if err := c.RegisterHandler("noop", func(map[string]any) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	spec := customSpec("task/beta", "v1", "noop")
	first, _ := c.Run(ctx, spec)
	if first.Outcome != OutcomeExecuted {
		t.Fatalf("first outcome = %s, want EXECUTED", first.Outcome)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows before second run = %d, want 0", n)
	}

	second, err := c.Run(ctx, spec)
	if err != nil {
		t.Fatalf("second Run returned error: %v", err)
	}
	if second.Outcome != OutcomeSkippedAlreadyDone {
		t.Fatalf("second outcome = %s, want SKIPPED_ALREADY_DONE (err: %v)", second.Outcome, second.Err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows after second run = %d, want 0", n)
	}
}

// Scenario 3 (contract §10): 8 concurrent "pods", each with its own connection
// pool, produce exactly one EXECUTED and seven SKIPPED_LOCKED, and the handler
// runs exactly once.
func TestConcurrentOnlyOneExecutes(t *testing.T) {
	ctx := context.Background()
	path := testDBPath(t)

	setup := openTestDB(t, path)
	if err := EnsureSchema(ctx, NewSQLite(setup)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	const pods = 8
	var calls atomic.Int32
	// The handler stays busy long enough that every pod reaches the acquisition
	// step while the lock is still held, which is what makes the "all losers are
	// SKIPPED_LOCKED" assertion deterministic.
	handler := func(map[string]any) error {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond)
		return nil
	}

	spec := customSpec("task/concurrent", "v1", "counting")
	results := make([]RunResult, pods)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < pods; i++ {
		// Every goroutine gets its own pool => its own real connection.
		db := openTestDB(t, path)
		coordinator := NewCoordinator(NewSQLite(db), fmt.Sprintf("pod-%d", i))
		if err := coordinator.RegisterHandler("counting", handler); err != nil {
			t.Fatalf("register handler: %v", err)
		}
		wg.Add(1)
		go func(index int, c *Coordinator) {
			defer wg.Done()
			<-start
			result, err := c.Run(ctx, spec)
			if err != nil {
				t.Errorf("Run returned error: %v", err)
			}
			results[index] = result
		}(i, coordinator)
	}

	close(start)
	wg.Wait()

	var executed, skippedLocked int
	for i, result := range results {
		switch result.Outcome {
		case OutcomeExecuted:
			executed++
		case OutcomeSkippedLocked:
			skippedLocked++
		default:
			t.Errorf("pod-%d outcome = %s (err: %v), want EXECUTED or SKIPPED_LOCKED",
				i, result.Outcome, result.Err)
		}
	}
	if executed != 1 {
		t.Fatalf("EXECUTED count = %d, want 1", executed)
	}
	if skippedLocked != pods-1 {
		t.Fatalf("SKIPPED_LOCKED count = %d, want %d", skippedLocked, pods-1)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if n := lockCount(t, setup); n != 0 {
		t.Fatalf("lock rows = %d, want 0", n)
	}
	if status, ok := versionStatus(t, setup, spec.TaskID, spec.Version); !ok || status != "success" {
		t.Fatalf("version record = (%q, %v), want (success, true)", status, ok)
	}
}

// Scenario 4 (contract §10): an already expired lock owned by another pod is
// taken over; the new pod executes and owns the row while it runs.
func TestExpiredLockTakeover(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	const taskID = "task/takeover"
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
		taskID, "old-pod",
		formatSQLiteTime(now.Add(-30*time.Minute)),
		formatSQLiteTime(now.Add(-20*time.Minute)),
	); err != nil {
		t.Fatalf("insert expired lock: %v", err)
	}

	c := NewCoordinator(store, "new-pod")
	observedOwner := ""
	if err := c.RegisterHandler("inspect", func(map[string]any) error {
		var queryErr error
		queryErr = db.QueryRow(
			`SELECT owner_id FROM task_execution_lock WHERE task_id = ?`, taskID).Scan(&observedOwner)
		return queryErr
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	result, err := c.Run(ctx, customSpec(taskID, "v1", "inspect"))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	if observedOwner != "new-pod" {
		t.Fatalf("lock owner during execution = %q, want new-pod", observedOwner)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows after run = %d, want 0", n)
	}
	if status, ok := versionStatus(t, db, taskID, "v1"); !ok || status != "success" {
		t.Fatalf("version record = (%q, %v), want (success, true)", status, ok)
	}

	var recordedOwner string
	if err := db.QueryRow(
		`SELECT owner_id FROM task_version_record WHERE task_id = ? AND version = ?`,
		taskID, "v1").Scan(&recordedOwner); err != nil {
		t.Fatalf("select version owner: %v", err)
	}
	if recordedOwner != "new-pod" {
		t.Fatalf("version owner = %q, want new-pod", recordedOwner)
	}
}

// A lock that has not expired must not be taken over.
func TestLockedByOtherOwnerSkips(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	const taskID = "task/held"
	now := time.Now().UTC()
	if _, err := db.Exec(
		`INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
		taskID, "other-pod",
		formatSQLiteTime(now),
		formatSQLiteTime(now.Add(10*time.Minute)),
	); err != nil {
		t.Fatalf("insert lock: %v", err)
	}

	var calls atomic.Int32
	c := NewCoordinator(store, "my-pod")
	if err := c.RegisterHandler("noop", func(map[string]any) error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	result, _ := c.Run(ctx, customSpec(taskID, "v1", "noop"))
	if result.Outcome != OutcomeSkippedLocked {
		t.Fatalf("outcome = %s, want SKIPPED_LOCKED (err: %v)", result.Outcome, result.Err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0", got)
	}
	if owner, ok := lockOwner(t, db, taskID); !ok || owner != "other-pod" {
		t.Fatalf("lock owner = (%q, %v), want (other-pod, true)", owner, ok)
	}
}

// Scenario 5 (contract §10): a failing handler yields FAILED, releases the lock
// and records status='failed' (not success).
func TestFailureReleasesLockAndNoSuccess(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	const taskID = "task/failing"
	c := NewCoordinator(store, "pod-a")
	if err := c.RegisterHandler("boom", func(map[string]any) error {
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	result, err := c.Run(ctx, customSpec(taskID, "v1", "boom"))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %s, want FAILED", result.Outcome)
	}
	if result.Err == nil || result.Err.Error() != "boom" {
		t.Fatalf("Err = %v, want boom", result.Err)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows = %d, want 0 (lock must be released on failure)", n)
	}
	status, ok := versionStatus(t, db, taskID, "v1")
	if !ok {
		t.Fatalf("no version record written for failed run")
	}
	if status != "failed" {
		t.Fatalf("version status = %q, want failed", status)
	}

	// A failed record must not block a retry once the handler works, and the
	// retry's success record must overwrite the failed one (Q4 upsert).
	c2 := NewCoordinator(store, "pod-b")
	if err := c2.RegisterHandler("ok", func(map[string]any) error { return nil }); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	retry, _ := c2.Run(ctx, customSpec(taskID, "v1", "ok"))
	if retry.Outcome != OutcomeExecuted {
		t.Fatalf("retry outcome = %s, want EXECUTED (err: %v)", retry.Outcome, retry.Err)
	}
	status, ok = versionStatus(t, db, taskID, "v1")
	if !ok || status != "success" {
		t.Fatalf("version record after retry = (%q, %v), want (success, true)", status, ok)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows after retry = %d, want 0", n)
	}
	var rows int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM task_version_record WHERE task_id = ? AND version = ?`,
		taskID, "v1").Scan(&rows); err != nil {
		t.Fatalf("count version records: %v", err)
	}
	if rows != 1 {
		t.Fatalf("version records = %d, want 1 (upsert, not insert)", rows)
	}
}

// Scenario 6 (contract §10): releasing must never delete a lock that has been
// taken over by another owner.
func TestReleaseDoesNotDeleteOtherOwnerLock(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	const taskID = "task/ownership"
	now := time.Now().UTC()
	acquired, err := store.TryAcquire(ctx, taskID, "pod-a", now, now.Add(5*time.Minute))
	if err != nil || !acquired {
		t.Fatalf("TryAcquire = (%v, %v), want (true, nil)", acquired, err)
	}

	// Simulate a takeover: the row now belongs to pod-b.
	if _, err := db.Exec(
		`UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ?`, "pod-b", taskID); err != nil {
		t.Fatalf("reassign lock: %v", err)
	}

	// A's release path must be a no-op.
	if err := releaseLock(ctx, store, taskID, "pod-a"); err != nil {
		t.Fatalf("releaseLock: %v", err)
	}
	owner, ok := lockOwner(t, db, taskID)
	if !ok {
		t.Fatalf("lock row was deleted, want it kept with owner pod-b")
	}
	if owner != "pod-b" {
		t.Fatalf("lock owner = %q, want pod-b", owner)
	}
}

// Same guarantee through the full Run path: the handler reassigns the lock to
// another owner while A still believes it holds it; A's deferred release must
// leave that row alone.
func TestReleaseThroughRunKeepsOtherOwnerLock(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	const taskID = "task/ownership-run"
	c := NewCoordinator(store, "pod-a")
	if err := c.RegisterHandler("steal", func(map[string]any) error {
		_, err := db.Exec(
			`UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ? AND owner_id = ?`,
			"pod-b", taskID, "pod-a")
		return err
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	result, _ := c.Run(ctx, customSpec(taskID, "v1", "steal"))
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	owner, ok := lockOwner(t, db, taskID)
	if !ok || owner != "pod-b" {
		t.Fatalf("lock = (%q, %v), want (pod-b, true)", owner, ok)
	}
}

// Step 3 of the algorithm: after winning the lock, a success record written in
// the meantime (takeover scenario) turns the run into SKIPPED_ALREADY_DONE and
// still releases the lock.
func TestDoubleCheckSkipsWhenSuccessAppeared(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	const taskID = "task/double-check"

	// Simulate the takeover race: a success record appears (written by the
	// previous owner) between our step 1 read and the double check of step 3.
	// The store decorator injects that write right after acquisition succeeds.
	racing := &racingStore{Store: store, afterAcquire: func(ctx context.Context) error {
		return store.RecordVersion(ctx, taskID, "v1", "success", "old-pod", time.Now().UTC())
	}}
	racingCoordinator := NewCoordinator(racing, "pod-a")
	if err := racingCoordinator.RegisterHandler("must-not-run", func(map[string]any) error {
		t.Errorf("handler ran although a success record appeared during takeover")
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	result, _ := racingCoordinator.Run(ctx, customSpec(taskID, "v1", "must-not-run"))
	if result.Outcome != OutcomeSkippedAlreadyDone {
		t.Fatalf("outcome = %s, want SKIPPED_ALREADY_DONE (err: %v)", result.Outcome, result.Err)
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows = %d, want 0 (deferred release must still run)", n)
	}
	if status, ok := versionStatus(t, db, taskID, "v1"); !ok || status != "success" {
		t.Fatalf("version record = (%q, %v), want (success, true)", status, ok)
	}
}

// racingStore decorates a Store and runs afterAcquire when the lock is won.
type racingStore struct {
	Store
	afterAcquire func(ctx context.Context) error
}

func (s *racingStore) TryAcquire(ctx context.Context, taskID, ownerID string, now, expires time.Time) (bool, error) {
	acquired, err := s.Store.TryAcquire(ctx, taskID, ownerID, now, expires)
	if err == nil && acquired && s.afterAcquire != nil {
		if hookErr := s.afterAcquire(ctx); hookErr != nil {
			return acquired, hookErr
		}
	}
	return acquired, err
}

func TestRunAllKeepsOrderAndIsolatesFailures(t *testing.T) {
	ctx := context.Background()
	_, store := newTestStore(t)

	c := NewCoordinator(store, "pod-a")
	if err := c.RegisterHandler("ok", func(map[string]any) error { return nil }); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	specs := []TaskSpec{
		customSpec("task/one", "v1", "ok"),
		customSpec("task/two", "v1", "missing-handler"),
		customSpec("task/three", "v1", "ok"),
	}
	results := c.RunAll(ctx, specs)
	if len(results) != len(specs) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(specs))
	}
	want := []Outcome{OutcomeExecuted, OutcomeFailed, OutcomeExecuted}
	for i, result := range results {
		if result.Outcome != want[i] {
			t.Fatalf("results[%d].Outcome = %s, want %s (err: %v)",
				i, result.Outcome, want[i], result.Err)
		}
		if result.TaskID != specs[i].TaskID {
			t.Fatalf("results[%d].TaskID = %q, want %q", i, result.TaskID, specs[i].TaskID)
		}
	}
	if got := results[1].Err; got == nil || got.Error() != `handler "missing-handler" is not registered` {
		t.Fatalf("results[1].Err = %v, want handler \"missing-handler\" is not registered", got)
	}
}

func TestRegisterHandlerDuplicate(t *testing.T) {
	_, store := newTestStore(t)
	c := NewCoordinator(store, "pod-a")
	if err := c.RegisterHandler("dup", func(map[string]any) error { return nil }); err != nil {
		t.Fatalf("first register: %v", err)
	}
	err := c.RegisterHandler("dup", func(map[string]any) error { return nil })
	if err == nil {
		t.Fatalf("second register returned nil, want error")
	}
}

func TestHandlerPanicBecomesFailed(t *testing.T) {
	ctx := context.Background()
	db, store := newTestStore(t)

	c := NewCoordinator(store, "pod-a")
	if err := c.RegisterHandler("panic", func(map[string]any) error {
		panic("handler exploded")
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	result, err := c.Run(ctx, customSpec("task/panic", "v1", "panic"))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %s, want FAILED", result.Outcome)
	}
	if result.Err == nil {
		t.Fatalf("Err = nil, want panic message")
	}
	if n := lockCount(t, db); n != 0 {
		t.Fatalf("lock rows = %d, want 0", n)
	}
}

func TestHandlerReceivesCopiedConfig(t *testing.T) {
	ctx := context.Background()
	_, store := newTestStore(t)

	c := NewCoordinator(store, "pod-a")
	var seen map[string]any
	if err := c.RegisterHandler("copy", func(config map[string]any) error {
		seen = config
		config["injected"] = true
		return nil
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}

	spec := TaskSpec{
		TaskID:         "task/copy",
		Version:        "v1",
		TimeoutSeconds: 30,
		Execution: Execution{
			Executor: "custom",
			Handler:  "copy",
			Config:   map[string]any{"ttl_seconds": 300},
		},
	}
	if result, _ := c.Run(ctx, spec); result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	if seen["ttl_seconds"] == nil {
		t.Fatalf("handler config = %v, want ttl_seconds present", seen)
	}
	if _, leaked := spec.Execution.Config["injected"]; leaked {
		t.Fatalf("handler mutated the spec config map; want a deep copy")
	}
}
