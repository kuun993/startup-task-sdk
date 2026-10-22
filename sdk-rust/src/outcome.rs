//! The verdict of one `run()` and the result it carries.

use std::fmt;
use std::time::Duration;

/// Verdict of one coordination attempt. The string form is the upper-case constant name (contract §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// The task was executed and succeeded.
    Executed,
    /// `(task_id, version)` already has a `status = 'success'` record.
    SkippedAlreadyDone,
    /// The execution lock was not acquired.
    SkippedLocked,
    /// The lock was acquired but execution failed (or its preconditions did).
    Failed,
}

impl Outcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Outcome::Executed => "EXECUTED",
            Outcome::SkippedAlreadyDone => "SKIPPED_ALREADY_DONE",
            Outcome::SkippedLocked => "SKIPPED_LOCKED",
            Outcome::Failed => "FAILED",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Return value of `run()`. Field names and meanings match the other language implementations (contract §2).
#[derive(Debug, Clone)]
pub struct RunResult {
    pub task_id: String,
    pub version: String,
    pub outcome: Outcome,
    /// Only set when `outcome == Failed`.
    pub error: Option<String>,
    /// Wall-clock time from entering `run()` to the end of the last release attempt.
    pub duration: Duration,
}

impl RunResult {
    pub fn duration_ms(&self) -> u64 {
        self.duration.as_millis() as u64
    }

    pub fn is_success(&self) -> bool {
        matches!(
            self.outcome,
            Outcome::Executed | Outcome::SkippedAlreadyDone | Outcome::SkippedLocked
        )
    }
}

impl fmt::Display for RunResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} task_id={} version={} duration_ms={}",
            self.outcome,
            self.task_id,
            self.version,
            self.duration_ms()
        )?;
        if let Some(err) = &self.error {
            write!(f, " error={}", err)?;
        }
        Ok(())
    }
}
