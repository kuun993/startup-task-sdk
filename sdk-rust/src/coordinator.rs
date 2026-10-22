//! Coordinator: follows the steps and their order from contract §5 exactly.

use std::any::Any;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::FutureExt;

use crate::executor;
use crate::outcome::{Outcome, RunResult};
use crate::spec::{Execution, TaskSpec};
use crate::store::{Store, StoreError};

/// Error type returned by caller-provided handlers.
pub type HandlerError = Box<dyn StdError + Send + Sync>;

/// Caller-provided handler: receives `execution.config` (`{}` when absent) and returns `Result<(), HandlerError>`.
pub type Handler = Box<dyn Fn(serde_json::Value) -> Result<(), HandlerError> + Send + Sync>;

/// Time source: UTC wall clock plus a monotonic clock. Injecting it lets tests pin `expires_at` and `duration`.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
    fn monotonic(&self) -> Instant;
}

/// Default clock: process wall clock (UTC) plus `std::time::Instant`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn monotonic(&self) -> Instant {
        Instant::now()
    }
}

/// Error returned by `register_handler`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterHandlerError {
    message: String,
}

impl RegisterHandlerError {
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RegisterHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for RegisterHandlerError {}

/// Coordinator: check version, acquire the lock, execute, record the result, release the lock.
pub struct Coordinator {
    store: Arc<dyn Store>,
    owner_id: String,
    clock: Arc<dyn Clock>,
    handlers: HashMap<String, Handler>,
}

/// Phase in which a failure happened; it decides the cleanup actions (contract §7):
/// `Acquire` (Q2/Q3 only) writes no version record and releases no lock; every other phase writes a `failed` record and still releases the lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Acquire,
    Execute,
}

struct CoordError {
    phase: Phase,
    message: String,
}

impl CoordError {
    fn acquire(message: impl Into<String>) -> Self {
        Self {
            phase: Phase::Acquire,
            message: message.into(),
        }
    }

    fn execute(message: impl Into<String>) -> Self {
        Self {
            phase: Phase::Execute,
            message: message.into(),
        }
    }
}

impl Coordinator {
    /// Build a coordinator driven by the system clock.
    pub fn new(store: Arc<dyn Store>, owner_id: impl Into<String>) -> Self {
        Self::with_clock(store, owner_id, Arc::new(SystemClock))
    }

    /// Uses `<hostname>/<uuid8>` as `owner_id` (fixed for the lifetime of the process).
    pub fn with_generated_owner(store: Arc<dyn Store>) -> Self {
        Self::new(store, default_owner_id())
    }

    /// Build a coordinator with an injected clock: every persisted timestamp and `RunResult.duration` comes from it.
    pub fn with_clock(store: Arc<dyn Store>, owner_id: impl Into<String>, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            owner_id: owner_id.into(),
            clock,
            handlers: HashMap::new(),
        }
    }

    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    pub fn store(&self) -> &Arc<dyn Store> {
        &self.store
    }

    /// Register a custom handler. Registering the same name twice returns `Err` (contract §2).
    pub fn register_handler(&mut self, name: &str, f: Handler) -> Result<(), RegisterHandlerError> {
        if self.handlers.contains_key(name) {
            return Err(RegisterHandlerError {
                message: format!("handler \"{}\" is already registered", name),
            });
        }
        self.handlers.insert(name.to_string(), f);
        Ok(())
    }

    /// Run a single task. Never panics; every failure converges into `Outcome::Failed`.
    pub async fn run(&self, spec: &TaskSpec) -> RunResult {
        let started = self.clock.monotonic();
        let mut lock_held = false;

        let (outcome, error) = match self.coordinate(spec, &mut lock_held).await {
            Ok(outcome) => (outcome, None),
            Err(err) => {
                if err.phase == Phase::Execute {
                    // A FAILED run always tries to write a status='failed' version record; a write failure is only logged.
                    self.record_failure(spec).await;
                }
                (Outcome::Failed, Some(err.message))
            }
        };

        // Finally-equivalent: every path (early return, execution error, handler panic) releases here.
        if lock_held {
            if let Err(err) = self.release_lock(&spec.task_id).await {
                tracing::warn!(
                    task_id = %spec.task_id,
                    owner_id = %self.owner_id,
                    error = %err,
                    "release failed"
                );
            }
        }

        RunResult {
            task_id: spec.task_id.clone(),
            version: spec.version.clone(),
            outcome,
            error,
            duration: self.clock.monotonic().saturating_duration_since(started),
        }
    }

    /// Run serially; a failing task never aborts the rest, and results keep the input order.
    pub async fn run_all(&self, specs: &[TaskSpec]) -> Vec<RunResult> {
        let mut results = Vec::with_capacity(specs.len());
        for spec in specs {
            results.push(self.run(spec).await);
        }
        results
    }

    /// Release the lock we hold (Q5 with its `owner_id` condition). `run` uses it for cleanup and tests call it directly.
    pub async fn release_lock(&self, task_id: &str) -> Result<(), StoreError> {
        self.store.release(task_id, &self.owner_id).await
    }

    async fn coordinate(
        &self,
        spec: &TaskSpec,
        lock_held: &mut bool,
    ) -> Result<Outcome, CoordError> {
        // Step 1: version check (only status = 'success' counts; a failed record does not block retries).
        // Database errors are passed through verbatim and handled as §7 requires ("FAILED always tries to write a failed record").
        let done = self
            .store
            .version_succeeded(&spec.task_id, &spec.version)
            .await
            .map_err(|e| CoordError::execute(e.to_string()))?;
        if done {
            tracing::info!(
                task_id = %spec.task_id,
                version = %spec.version,
                "skip already done"
            );
            return Ok(Outcome::SkippedAlreadyDone);
        }

        // Step 2: acquire the lock. Q2 is an atomic INSERT with duplicate-key detection; on conflict we try the Q3 takeover.
        // `now` is sampled once per acquire round; acquired_at and expires_at share it.
        let now = self.clock.now();
        let expires_at = now + lock_ttl(spec.timeout_seconds);
        let acquired = self
            .store
            .try_acquire(&spec.task_id, &self.owner_id, now, expires_at)
            .await
            .map_err(|e| CoordError::acquire(e.to_string()))?;

        if !acquired {
            tracing::info!(
                task_id = %spec.task_id,
                owner_id = %self.owner_id,
                "skip locked"
            );
            return Ok(Outcome::SkippedLocked);
        }
        *lock_held = true;
        tracing::info!(
            task_id = %spec.task_id,
            owner_id = %self.owner_id,
            "lock acquired"
        );

        // Step 3: double check (in a takeover the previous owner may have recorded success already).
        let done = self
            .store
            .version_succeeded(&spec.task_id, &spec.version)
            .await
            .map_err(|e| CoordError::execute(e.to_string()))?;
        if done {
            tracing::info!(
                task_id = %spec.task_id,
                version = %spec.version,
                "skip already done"
            );
            return Ok(Outcome::SkippedAlreadyDone);
        }

        // Steps 4 + 4a: execute and record. A panic! inside a handler must not escape run():
        // catch_unwind turns it into an execution failure, then we write the failed record and release the lock as usual.
        match AssertUnwindSafe(self.execute_and_record(spec))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => Ok(Outcome::Executed),
            Ok(Err(message)) => Err(CoordError::execute(message)),
            Err(payload) => Err(CoordError::execute(format!(
                "handler panicked: {}",
                panic_payload_message(payload.as_ref())
            ))),
        }
    }

    /// Step 4 (execute) plus step 4a (record success).
    async fn execute_and_record(&self, spec: &TaskSpec) -> Result<(), String> {
        self.execute(spec).await?;

        let finished_at = self.clock.now();
        self.store
            .record_version(
                &spec.task_id,
                &spec.version,
                "success",
                finished_at,
                &self.owner_id,
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn execute(&self, spec: &TaskSpec) -> Result<(), String> {
        match &spec.execution {
            Execution::Http(cfg) => executor::execute_http(spec, cfg)
                .await
                .map_err(|e| e.message().to_string()),
            Execution::Custom(cfg) => {
                let handler = self
                    .handlers
                    .get(&cfg.handler)
                    .ok_or_else(|| format!("handler \"{}\" is not registered", cfg.handler))?;
                // config is passed to the handler as-is (deep copy).
                handler(cfg.config.clone()).map_err(|e| e.to_string())
            }
        }
    }

    async fn record_failure(&self, spec: &TaskSpec) {
        let now = self.clock.now();
        if let Err(err) = self
            .store
            .record_version(&spec.task_id, &spec.version, "failed", now, &self.owner_id)
            .await
        {
            tracing::warn!(
                task_id = %spec.task_id,
                version = %spec.version,
                error = %err,
                "failed to record failed version"
            );
        }
    }
}

/// Extract the text of a panic payload for `handler panicked: <msg>`.
fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        return (*text).to_string();
    }
    if let Some(text) = payload.downcast_ref::<String>() {
        return text.clone();
    }
    "unknown panic payload".to_string()
}

/// Defensive cap on the lock TTL (100 years). `timeout_seconds` is caller-provided and extreme values overflow chrono,
/// while `run` must not panic, so we clamp here (contract semantics still hold: `expires_at = now + timeout_seconds`).
const MAX_LOCK_TTL_SECONDS: u64 = 100 * 365 * 24 * 3600;

fn lock_ttl(timeout_seconds: u64) -> ChronoDuration {
    ChronoDuration::seconds(timeout_seconds.min(MAX_LOCK_TTL_SECONDS) as i64)
}

/// Process-wide owner_id: `<hostname>/<uuid8>`, fixed after the first call
/// (so releasing the lock always matches our own acquisition, contract §2).
static PROCESS_OWNER_ID: OnceLock<String> = OnceLock::new();

pub fn default_owner_id() -> String {
    PROCESS_OWNER_ID.get_or_init(build_owner_id).clone()
}

fn build_owner_id() -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string());
    format!("{}/{:08x}", host, random_u32())
}

fn random_u32() -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = nanos ^ ((std::process::id() as u64) << 32) ^ 0x9E37_79B9_7F4A_7C15;
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    (x >> 32) as u32
}
