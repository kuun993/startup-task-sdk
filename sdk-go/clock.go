package startuptask

import "time"

// Clock supplies the current time to a Coordinator. The contract (§4.2) requires
// every timestamp to be produced by the SDK clock and bound as a SQL parameter,
// which is also what makes the clock injectable in tests.
type Clock interface {
	Now() time.Time
}

// SystemClock is the default Clock: the process wall clock.
type SystemClock struct{}

// Now implements Clock. The monotonic reading is kept on purpose: Run derives
// RunResult.Duration by subtracting two Clock readings, and a monotonic source is
// immune to wall-clock jumps (NTP/DST). Note that on platforms whose clock quantum
// is coarse (about 1ms on this Windows host) a sub-millisecond run can still
// measure as exactly 0; inject a Clock when an exact duration is required.
// Timestamps are converted to UTC by the coordinator right before they are bound
// as SQL parameters.
func (SystemClock) Now() time.Time { return time.Now() }

// maxLeaseSeconds bounds timeout_seconds when computing expires_at (100 years);
// extreme values are clamped instead of overflowing time.Duration.
const maxLeaseSeconds = 100 * 365 * 24 * 3600

// leaseDuration converts timeout_seconds into a Duration, clamped to 100 years.
func leaseDuration(seconds int) time.Duration {
	if seconds <= 0 {
		return 0
	}
	if seconds > maxLeaseSeconds {
		seconds = maxLeaseSeconds
	}
	return time.Duration(seconds) * time.Second
}
