package startuptask

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Handler is a caller-registered executor body. A nil return means success; any
// non-nil error (or a panic) fails the task.
type Handler func(config map[string]any) error

// Coordinator runs task definitions against a Store, implementing the
// coordination algorithm of docs/design.md §5.
type Coordinator struct {
	store    Store
	ownerID  string
	handlers map[string]Handler
	mu       sync.RWMutex
	clock    Clock
	logger   *slog.Logger
}

// NewCoordinator builds a Coordinator driven by SystemClock. When ownerID is
// empty it defaults to "<hostname>/<random8>", computed once per process, so a
// release always matches the lock this process acquired.
func NewCoordinator(store Store, ownerID string) *Coordinator {
	return NewCoordinatorWithClock(store, ownerID, SystemClock{})
}

// NewCoordinatorWithClock builds a Coordinator with an injected Clock. All
// timestamps written by the coordinator (acquired_at, expires_at, executed_at,
// RunResult.Duration) derive from this clock, so tests can pin them.
func NewCoordinatorWithClock(store Store, ownerID string, clock Clock) *Coordinator {
	if ownerID == "" {
		ownerID = defaultOwnerID()
	}
	if clock == nil {
		clock = SystemClock{}
	}
	return &Coordinator{
		store:    store,
		ownerID:  ownerID,
		handlers: make(map[string]Handler),
		clock:    clock,
		logger:   slog.Default(),
	}
}

// SetLogger replaces the coordinator's logger (defaults to slog.Default()).
func (c *Coordinator) SetLogger(logger *slog.Logger) {
	if logger != nil {
		c.logger = logger
	}
}

// OwnerID is the identity used in task_execution_lock.owner_id.
func (c *Coordinator) OwnerID() string { return c.ownerID }

// RegisterHandler makes a custom handler available to executor=custom specs.
// Registering the same name twice is an error.
func (c *Coordinator) RegisterHandler(name string, fn Handler) error {
	if name == "" {
		return errors.New("handler name must not be empty")
	}
	if fn == nil {
		return fmt.Errorf("handler %q must not be nil", name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.handlers[name]; exists {
		return fmt.Errorf("handler %q is already registered", name)
	}
	c.handlers[name] = fn
	return nil
}

// RunAll runs the specs serially, in order. A failing task never aborts the
// batch and results are returned in input order.
func (c *Coordinator) RunAll(ctx context.Context, specs []TaskSpec) []RunResult {
	results := make([]RunResult, 0, len(specs))
	for _, spec := range specs {
		result, _ := c.Run(ctx, spec)
		results = append(results, result)
	}
	return results
}

// Run executes one task following the contract algorithm:
//
//	Q1 check -> Q2 acquire (or Q3 takeover) -> Q1 double check -> execute ->
//	Q4 record -> (defer) Q5 release.
//
// It never panics and never returns a non-nil error: every failure is reported
// through RunResult.Outcome == FAILED and RunResult.Err.
func (c *Coordinator) Run(ctx context.Context, spec TaskSpec) (result RunResult, runErr error) {
	start := c.clock.Now()
	result = RunResult{TaskID: spec.TaskID, Version: spec.Version}

	lockHeld := false
	log := c.logger.With(
		"task_id", spec.TaskID,
		"version", spec.Version,
		"owner_id", c.ownerID,
	)

	defer func() {
		// Q5 must run on every exit path, including the early returns of
		// steps 1 and 3. A failed release only logs; the outcome stands.
		if lockHeld {
			if err := releaseLock(ctx, c.store, spec.TaskID, c.ownerID); err != nil {
				log.Warn("release failed", "error", err)
			}
		}
		result.Duration = c.clock.Now().Sub(start)
	}()

	if spec.TaskID == "" {
		result.Outcome = OutcomeFailed
		result.Err = errors.New("task.task_id is required")
		return result, nil
	}
	if spec.Version == "" {
		result.Outcome = OutcomeFailed
		result.Err = errors.New("task.version is required")
		return result, nil
	}

	// Step 1: version check. Only status='success' blocks a rerun.
	succeeded, err := c.store.VersionSucceeded(ctx, spec.TaskID, spec.Version)
	if err != nil {
		log.Error("version check failed", "error", err)
		result.Outcome = OutcomeFailed
		result.Err = err
		c.recordFailure(ctx, log, spec)
		return result, nil
	}
	if succeeded {
		log.Info("skip already done")
		result.Outcome = OutcomeSkippedAlreadyDone
		return result, nil
	}

	// Step 2: acquire. `now` is read once; acquired_at and expires_at share it.
	now := c.clock.Now().UTC()
	expires := now.Add(leaseDuration(spec.timeoutSeconds()))
	acquired, err := c.store.TryAcquire(ctx, spec.TaskID, c.ownerID, now, expires)
	if err != nil {
		// Contract §7: a non-conflict error while acquiring leaves the lock
		// state unknown, so neither a version record nor a release is attempted.
		log.Error("acquire failed", "error", err)
		result.Outcome = OutcomeFailed
		result.Err = err
		return result, nil
	}
	if !acquired {
		log.Info("skip locked")
		result.Outcome = OutcomeSkippedLocked
		return result, nil
	}
	lockHeld = true
	log.Info("lock acquired", "expires_at", expires.UTC().Format(time.RFC3339Nano))

	// Step 3: double check. In the takeover case the previous owner may have
	// finished and recorded success after our step 1 read.
	succeeded, err = c.store.VersionSucceeded(ctx, spec.TaskID, spec.Version)
	if err != nil {
		log.Error("version check failed", "error", err)
		result.Outcome = OutcomeFailed
		result.Err = err
		c.recordFailure(ctx, log, spec)
		return result, nil
	}
	if succeeded {
		log.Info("skip already done", "stage", "double-check")
		result.Outcome = OutcomeSkippedAlreadyDone
		return result, nil
	}

	// Step 4: execute.
	if err := c.execute(ctx, spec); err != nil {
		log.Error("execution failed", "error", err)
		result.Outcome = OutcomeFailed
		result.Err = err
		c.recordFailure(ctx, log, spec)
		return result, nil
	}

	// Step 4a: record success.
	if err := c.store.RecordVersion(ctx, spec.TaskID, spec.Version, statusSuccess, c.ownerID, c.clock.Now().UTC()); err != nil {
		log.Error("record success failed", "error", err)
		result.Outcome = OutcomeFailed
		result.Err = err
		c.recordFailure(ctx, log, spec)
		return result, nil
	}

	log.Info("executed")
	result.Outcome = OutcomeExecuted
	return result, nil
}

// recordFailure writes a status='failed' version record. A failure to write it
// is logged only and never changes the outcome.
func (c *Coordinator) recordFailure(ctx context.Context, log *slog.Logger, spec TaskSpec) {
	if err := c.store.RecordVersion(ctx, spec.TaskID, spec.Version, statusFailed, c.ownerID, c.clock.Now().UTC()); err != nil {
		log.Warn("record failed status failed", "error", err)
	}
}

// execute dispatches to the executor declared by the spec and converts a panic
// into an error (a handler must not take the process down).
func (c *Coordinator) execute(ctx context.Context, spec TaskSpec) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("executor panicked: %v", r)
		}
	}()

	switch spec.Execution.Executor {
	case "http":
		return c.executeHTTP(ctx, spec)
	case "custom":
		return c.executeCustom(spec)
	default:
		return fmt.Errorf("execution.executor must be one of [http, custom], got %q", spec.Execution.Executor)
	}
}

func (c *Coordinator) executeCustom(spec TaskSpec) (err error) {
	name := spec.Execution.Handler
	if name == "" {
		return errors.New("execution.handler is required")
	}
	c.mu.RLock()
	handler, ok := c.handlers[name]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("handler %q is not registered", name)
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler %q panicked: %v", name, r)
		}
	}()
	return handler(deepCopyMap(spec.Execution.Config))
}

func deepCopyMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		return deepCopyMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}

// defaultOwnerID is computed once per process: a later release must carry the
// same owner_id as the acquisition, and contract §2 makes it process-stable.
var (
	ownerOnce     sync.Once
	cachedOwnerID string
)

func defaultOwnerID() string {
	ownerOnce.Do(func() {
		cachedOwnerID = buildOwnerID()
	})
	return cachedOwnerID
}

func buildOwnerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s/%08x", host, uint32(time.Now().UnixNano()))
	}
	return host + "/" + hex.EncodeToString(buf)
}
