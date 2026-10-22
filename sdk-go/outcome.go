package startuptask

import "time"

// Outcome is the conclusion of a single Run. The string values are part of the
// cross-language contract and must not change.
type Outcome string

const (
	// OutcomeExecuted means this run acquired the lock, executed the task and
	// recorded a status=success version record.
	OutcomeExecuted Outcome = "EXECUTED"
	// OutcomeSkippedAlreadyDone means a status=success record already existed
	// for (task_id, version).
	OutcomeSkippedAlreadyDone Outcome = "SKIPPED_ALREADY_DONE"
	// OutcomeSkippedLocked means this run did not obtain the execution right.
	OutcomeSkippedLocked Outcome = "SKIPPED_LOCKED"
	// OutcomeFailed means the execution right was obtained but the execution
	// (or a precondition of it) failed.
	OutcomeFailed Outcome = "FAILED"
)

func (o Outcome) String() string { return string(o) }

// RunResult is the result of one Run. Field names mirror the contract's
// task_id / version / outcome / error / duration_ms.
type RunResult struct {
	TaskID   string
	Version  string
	Outcome  Outcome
	Err      error
	Duration time.Duration
}

// DurationMs is the wall-clock duration in milliseconds, measured from entering
// Run until the last release-lock attempt finished.
func (r RunResult) DurationMs() float64 {
	return float64(r.Duration) / float64(time.Millisecond)
}

// Failed reports whether the run ended in OutcomeFailed.
func (r RunResult) Failed() bool { return r.Outcome == OutcomeFailed }
