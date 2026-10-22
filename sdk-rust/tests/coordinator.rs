//! The six scenarios of contract §10 (real SQLite SQL) plus error-convergence cases.
//!
//! Every test uses its own temporary database file with `journal_mode=WAL` and `busy_timeout=10s`,
//! and a pool with `max_connections = 4`; the concurrency case uses real parallel connections, not one serialised connection.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use startup_task_sdk::{
    ensure_schema, load_spec, Clock, Coordinator, HandlerError, Outcome, SqliteStore, Store,
    TaskSpec,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Isolated temporary database: returns a `TempDir` (kept alive) plus a pool.
async fn setup_db() -> (TempDir, SqlitePool) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().join("startup-task-test.db");

    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(10));

    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("connect sqlite");

    let store = SqliteStore::new(pool.clone());
    ensure_schema(&store).await.expect("ensure schema");

    (dir, pool)
}

fn sqlite_store(pool: &SqlitePool) -> Arc<SqliteStore> {
    Arc::new(SqliteStore::new(pool.clone()))
}

fn coordinator(store: Arc<SqliteStore>, owner: &str) -> Arc<Coordinator> {
    Arc::new(Coordinator::new(store as Arc<dyn Store>, owner))
}

fn custom_spec(task_id: &str, version: &str, handler: &str) -> TaskSpec {
    let yaml = format!(
        "task_id: {}\n\
         version: \"{}\"\n\
         timeout_seconds: 30\n\
         execution:\n\
         \x20 executor: custom\n\
         \x20 handler: {}\n",
        task_id, version, handler
    );
    load_spec(&yaml).expect("spec should be valid")
}

fn sqlite_time(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

async fn lock_count(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_execution_lock")
        .fetch_one(pool)
        .await
        .expect("count locks")
}

async fn lock_owner(pool: &SqlitePool, task_id: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>("SELECT owner_id FROM task_execution_lock WHERE task_id = ?")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("select lock owner")
}

async fn version_status(pool: &SqlitePool, task_id: &str, version: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT status FROM task_version_record WHERE task_id = ? AND version = ?",
    )
    .bind(task_id)
    .bind(version)
    .fetch_optional(pool)
    .await
    .expect("select version status")
}

async fn insert_lock(
    pool: &SqlitePool,
    task_id: &str,
    owner: &str,
    acquired_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) \
         VALUES (?, ?, ?, ?)",
    )
    .bind(task_id)
    .bind(owner)
    .bind(sqlite_time(acquired_at))
    .bind(sqlite_time(expires_at))
    .execute(pool)
    .await
    .expect("insert lock");
}

async fn delete_lock(pool: &SqlitePool, task_id: &str) {
    sqlx::query("DELETE FROM task_execution_lock WHERE task_id = ?")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("delete lock");
}

async fn update_lock_owner(pool: &SqlitePool, task_id: &str, owner: &str) {
    sqlx::query("UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ?")
        .bind(owner)
        .bind(task_id)
        .execute(pool)
        .await
        .expect("update lock owner");
}

// ---------------------------------------------------------------------------
// Scenario 1: first_run_executed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn first_run_executed() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut coordinator = Coordinator::new(store as Arc<dyn Store>, "pod-a");
    coordinator
        .register_handler(
            "warm_up_cache",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .expect("register handler");

    let spec = custom_spec("cache/warm-up", "1", "warm_up_cache");
    let result = coordinator.run(&spec).await;

    assert_eq!(result.outcome, Outcome::Executed);
    assert_eq!(result.outcome.as_str(), "EXECUTED");
    assert!(result.error.is_none());
    assert_eq!(result.task_id, "cache/warm-up");
    assert_eq!(result.version, "1");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        version_status(&pool, "cache/warm-up", "1").await.as_deref(),
        Some("success")
    );
    assert_eq!(lock_count(&pool).await, 0, "the lock must be released");

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Scenario 2: second_run_skipped_already_done
// ---------------------------------------------------------------------------

#[tokio::test]
async fn second_run_skipped_already_done() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut instance = Coordinator::new(store as Arc<dyn Store>, "pod-a");
    instance
        .register_handler(
            "warm_up_cache",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .expect("register handler");

    let spec = custom_spec("cache/warm-up", "1", "warm_up_cache");

    assert_eq!(lock_count(&pool).await, 0, "the lock table is empty before the run");
    let first = instance.run(&spec).await;
    assert_eq!(first.outcome, Outcome::Executed);
    let second = instance.run(&spec).await;
    assert_eq!(second.outcome, Outcome::SkippedAlreadyDone);
    assert_eq!(second.outcome.as_str(), "SKIPPED_ALREADY_DONE");
    assert!(second.error.is_none());

    assert_eq!(calls.load(Ordering::SeqCst), 1, "the handler is not called after a success record");
    assert_eq!(lock_count(&pool).await, 0, "the lock table stays empty after the run (the skip path writes no lock)");
    assert_eq!(
        version_status(&pool, "cache/warm-up", "1").await.as_deref(),
        Some("success")
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Scenario 3: concurrent_only_one_executes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn concurrent_only_one_executes() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);

    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let spec = Arc::new(custom_spec("cluster.slow-init", "1", "slow_task"));

    // Eight independent pods: each has its own Coordinator and owner_id, sharing one store/pool
    // (real parallel connections, not one serialised connection).
    let mut handles = Vec::new();
    for index in 0..8 {
        let counter = calls.clone();
        let finished = completed.clone();
        let waiting = completed.clone();
        let mut coordinator = Coordinator::new(
            store.clone() as Arc<dyn Store>,
            format!("pod-{}", index),
        );
        coordinator
            .register_handler(
                "slow_task",
                Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                    counter.fetch_add(1, Ordering::SeqCst);
                    // While holding the lock, wait until the other seven pods finish their own run (they get SKIPPED_LOCKED),
                    // so "1 EXECUTED + 7 SKIPPED_LOCKED" does not depend on scheduling speed.
                    // block_in_place tells the runtime that we block: it hands this worker's
                    // task queue and I/O driver to another worker. A bare std::thread::sleep
                    // would stall the scheduler too and the other seven pods would never finish.
                    tokio::task::block_in_place(|| {
                        let deadline = std::time::Instant::now() + Duration::from_secs(10);
                        while waiting.load(Ordering::SeqCst) < 7
                            && std::time::Instant::now() < deadline
                        {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                    });
                    Ok(())
                }),
            )
            .expect("register handler");
        let coordinator = Arc::new(coordinator);
        let spec = spec.clone();
        handles.push(tokio::spawn(async move {
            let result = coordinator.run(&spec).await;
            finished.fetch_add(1, Ordering::SeqCst);
            result
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.expect("task should not panic"));
    }

    let executed = results
        .iter()
        .filter(|r| r.outcome == Outcome::Executed)
        .count();
    let locked = results
        .iter()
        .filter(|r| r.outcome == Outcome::SkippedLocked)
        .count();

    assert_eq!(executed, 1, "exactly one EXECUTED, actual results: {:?}", results);
    assert_eq!(locked, 7, "all the others are SKIPPED_LOCKED, actual results: {:?}", results);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the handler is called exactly once");
    assert_eq!(lock_count(&pool).await, 0, "the lock must be released");
    assert_eq!(
        version_status(&pool, "cluster.slow-init", "1").await.as_deref(),
        Some("success")
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Scenario 4: expired_lock_takeover
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_lock_takeover() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let task_id = "cluster.publish-config";

    // Insert an already expired lock owned by a different pod.
    let now = Utc::now();
    insert_lock(
        &pool,
        task_id,
        "pod-old",
        now - chrono::Duration::seconds(120),
        now - chrono::Duration::seconds(60),
    )
    .await;

    // Takeover semantics: try_acquire returns true and the lock owner becomes the new pod (the lock is still held).
    let acquired = store
        .try_acquire(
            task_id,
            "pod-new",
            now,
            now + chrono::Duration::seconds(30),
        )
        .await
        .expect("takeover should not error");
    assert!(acquired, "an expired lock must be takeable");
    assert_eq!(
        lock_owner(&pool, task_id).await.as_deref(),
        Some("pod-new"),
        "the lock owner must become the new pod"
    );

    // Reset it to an expired lock and run the full path (Q2 conflict, Q3 takeover, execute, Q5 release).
    delete_lock(&pool, task_id).await;
    insert_lock(
        &pool,
        task_id,
        "pod-old",
        now - chrono::Duration::seconds(120),
        now - chrono::Duration::seconds(60),
    )
    .await;

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();

    // Assertion inside the handler: while executing, the lock row belongs to the new pod.
    let observed_owner: Arc<std::sync::Mutex<Option<Option<String>>>> =
        Arc::new(std::sync::Mutex::new(None));
    let observed_in_handler = observed_owner.clone();
    let pool_in_handler = pool.clone();
    let task_id_owned = task_id.to_string();

    let mut builder = Coordinator::new(store as Arc<dyn Store>, "pod-new");
    builder
        .register_handler(
            "publish_config",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                counter.fetch_add(1, Ordering::SeqCst);

                // The handler is synchronous but the query needs the pool: block in place on this worker to run it.
                let handle = tokio::runtime::Handle::current();
                let pool = pool_in_handler.clone();
                let task = task_id_owned.clone();
                let owner = tokio::task::block_in_place(|| {
                    handle.block_on(async move { lock_owner(&pool, &task).await })
                });

                *observed_in_handler.lock().expect("lock observed") = Some(owner.clone());
                assert_eq!(
                    owner.as_deref(),
                    Some("pod-new"),
                    "the lock must belong to the taking-over pod during execution"
                );
                Ok(())
            }),
        )
        .expect("register handler");

    let spec = custom_spec(task_id, "7", "publish_config");
    let result = builder.run(&spec).await;

    assert_eq!(
        result.outcome,
        Outcome::Executed,
        "a takeover must really execute, error = {:?}",
        result.error
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed_owner.lock().expect("lock observed").clone(),
        Some(Some("pod-new".to_string())),
        "the handler must observe the new pod holding the lock"
    );
    assert_eq!(
        version_status(&pool, task_id, "7").await.as_deref(),
        Some("success")
    );
    assert_eq!(lock_count(&pool).await, 0, "the taken-over lock is released after execution");

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Scenario 5: failure_releases_lock_and_no_success
// ---------------------------------------------------------------------------

#[tokio::test]
async fn failure_releases_lock_and_no_success() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);

    // Fail once, succeed next: proves a failed record does not block a retry and success eventually overwrites failed.
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");
    builder
        .register_handler(
            "flaky",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(Box::new(TestError("boom from handler")))
                } else {
                    Ok(())
                }
            }),
        )
        .expect("register handler");

    let spec = custom_spec("cache/warm-up", "9", "flaky");
    let result = builder.run(&spec).await;

    assert_eq!(result.outcome, Outcome::Failed);
    assert_eq!(result.outcome.as_str(), "FAILED");
    let error = result.error.expect("FAILED must carry an error");
    assert!(error.contains("boom from handler"), "error = {}", error);

    assert_eq!(lock_count(&pool).await, 0, "the failure path must release the lock too");
    assert_eq!(
        version_status(&pool, "cache/warm-up", "9").await.as_deref(),
        Some("failed"),
        "a failure must leave a status='failed' record"
    );

    // Retry the same version: a failed record does not block it (Q1 only honours success) and success overwrites failed.
    let retry = builder.run(&spec).await;
    assert_eq!(retry.outcome, Outcome::Executed, "error = {:?}", retry.error);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "the retry must really execute again");
    assert_eq!(lock_count(&pool).await, 0);
    assert_eq!(
        version_status(&pool, "cache/warm-up", "9").await.as_deref(),
        Some("success"),
        "after a successful retry success overwrites failed"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Scenario 6: release_does_not_delete_other_owner_lock
// ---------------------------------------------------------------------------

#[tokio::test]
async fn release_does_not_delete_other_owner_lock() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let task_id = "cluster.rotate-key";

    let coordinator_a = coordinator(store.clone(), "pod-a");

    // A acquires the lock.
    let now = Utc::now();
    let acquired = store
        .try_acquire(
            task_id,
            "pod-a",
            now,
            now + chrono::Duration::seconds(30),
        )
        .await
        .expect("acquire");
    assert!(acquired);
    assert_eq!(lock_owner(&pool, task_id).await.as_deref(), Some("pod-a"));

    // Simulate a takeover: the lock row is reassigned to B.
    update_lock_owner(&pool, task_id, "pod-b").await;

    // Trigger A's release path (Q5 carries the owner_id condition).
    coordinator_a
        .release_lock(task_id)
        .await
        .expect("release should not error");

    assert_eq!(
        lock_owner(&pool, task_id).await.as_deref(),
        Some("pod-b"),
        "A must not delete B's lock"
    );
    assert_eq!(lock_count(&pool).await, 1);

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Extra: error convergence and never panicking
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct TestError(&'static str);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for TestError {}

/// Temporarily silences the panic hook (restored on Drop) for tests that panic on purpose.
struct SilencePanics(Option<Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>>);

impl SilencePanics {
    fn new() -> Self {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        Self(Some(previous))
    }
}

impl Drop for SilencePanics {
    fn drop(&mut self) {
        if let Some(hook) = self.0.take() {
            std::panic::set_hook(hook);
        }
    }
}

#[tokio::test]
async fn handler_panic_becomes_failed_and_releases_lock() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);

    // This test panics inside the handler on purpose; silence the default hook to keep the output clean.
    let _silence = SilencePanics::new();

    let mut builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");
    builder
        .register_handler(
            "panics",
            Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> {
                panic!("boom in handler");
            }),
        )
        .expect("register handler");

    let spec = custom_spec("cache/warm-up", "panic-1", "panics");
    let result = builder.run(&spec).await;

    assert_eq!(result.outcome, Outcome::Failed, "a handler panic must converge into FAILED");
    let error = result.error.expect("FAILED must carry an error");
    assert_eq!(error, "handler panicked: boom in handler");

    assert_eq!(lock_count(&pool).await, 0, "the panic path must release the lock too");
    assert_ne!(
        version_status(&pool, "cache/warm-up", "panic-1")
            .await
            .as_deref(),
        Some("success"),
        "a panic must not leave a success record"
    );
    assert_eq!(
        version_status(&pool, "cache/warm-up", "panic-1")
            .await
            .as_deref(),
        Some("failed")
    );

    pool.close().await;
}

#[tokio::test]
async fn unregistered_handler_fails() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");

    let spec = custom_spec("cache/warm-up", "1", "nobody_registered_me");
    let result = builder.run(&spec).await;

    assert_eq!(result.outcome, Outcome::Failed);
    assert_eq!(
        result.error.as_deref(),
        Some("handler \"nobody_registered_me\" is not registered")
    );
    assert_eq!(lock_count(&pool).await, 0);

    pool.close().await;
}

#[tokio::test]
async fn duplicate_handler_registration_is_rejected() {
    let (_dir, pool) = setup_db().await;
    let mut builder = Coordinator::new(sqlite_store(&pool) as Arc<dyn Store>, "pod-a");

    let handler = || -> Box<dyn Fn(serde_json::Value) -> Result<(), HandlerError> + Send + Sync> {
        Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> { Ok(()) })
    };

    builder
        .register_handler("dupe", handler())
        .expect("first registration");
    let err = builder
        .register_handler("dupe", handler())
        .expect_err("duplicate registration must fail");
    assert!(err.to_string().contains("dupe"), "err = {}", err);

    pool.close().await;
}

#[tokio::test]
async fn http_missing_env_var_fails_with_contract_message() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");

    std::env::remove_var("STARTUP_TASK_SDK_MISSING_ENV");
    let yaml = "task_id: config.publish-app-settings\n\
                version: \"1\"\n\
                timeout_seconds: 5\n\
                execution:\n\
                \x20 executor: http\n\
                \x20 config:\n\
                \x20   method: POST\n\
                \x20   url: http://127.0.0.1:1/api/${STARTUP_TASK_SDK_MISSING_ENV}\n";
    let spec = load_spec(yaml).expect("spec should be valid");

    let result = builder.run(&spec).await;
    assert_eq!(result.outcome, Outcome::Failed);
    assert_eq!(
        result.error.as_deref(),
        Some("environment variable \"STARTUP_TASK_SDK_MISSING_ENV\" is not set")
    );

    pool.close().await;
}

#[tokio::test]
async fn http_transport_error_is_failed() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");

    // Nothing listens on port 1, so the connection fails fast.
    let yaml = "task_id: config.publish-app-settings\n\
                version: \"2\"\n\
                timeout_seconds: 5\n\
                execution:\n\
                \x20 executor: http\n\
                \x20 config:\n\
                \x20   method: POST\n\
                \x20   url: http://127.0.0.1:1/api/v1/apps/demo-app/settings\n";
    let spec = load_spec(yaml).expect("spec should be valid");

    let result = builder.run(&spec).await;
    assert_eq!(result.outcome, Outcome::Failed);
    let error = result.error.expect("FAILED must carry an error");
    assert!(
        error.starts_with("http request failed: "),
        "error = {}",
        error
    );
    assert_eq!(
        version_status(&pool, "config.publish-app-settings", "2")
            .await
            .as_deref(),
        Some("failed")
    );

    pool.close().await;
}

#[tokio::test]
async fn run_all_keeps_order_and_isolates_failures() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);

    let mut builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");
    builder
        .register_handler(
            "ok_task",
            Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> { Ok(()) }),
        )
        .expect("register handler");
    builder
        .register_handler(
            "bad_task",
            Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> {
                Err(Box::new(TestError("nope")))
            }),
        )
        .expect("register handler");

    let specs = vec![
        custom_spec("task.one", "1", "ok_task"),
        custom_spec("task.two", "1", "bad_task"),
        custom_spec("task.three", "1", "ok_task"),
    ];

    let results = builder.run_all(&specs).await;
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].task_id, "task.one");
    assert_eq!(results[1].task_id, "task.two");
    assert_eq!(results[2].task_id, "task.three");
    assert_eq!(
        results.iter().map(|r| r.outcome).collect::<Vec<_>>(),
        vec![Outcome::Executed, Outcome::Failed, Outcome::Executed]
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Extra: real HTTP executor requests against a local mock server
// ---------------------------------------------------------------------------

/// Start a minimal HTTP/1.1 server that answers exactly once; returns (base_url, captured raw request).
async fn spawn_mock_server(
    status: u16,
    response_body: &'static str,
) -> (String, Arc<tokio::sync::Mutex<String>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let addr = listener.local_addr().expect("mock addr");
    let captured = Arc::new(tokio::sync::Mutex::new(String::new()));
    let captured_in_task = captured.clone();

    tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };

        let mut data: Vec<u8> = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let read = match socket.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            data.extend_from_slice(&buf[..read]);
            if let Some(header_end) = find_subslice(&data, b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&data[..header_end]).to_lowercase();
                let expected = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if data.len() - (header_end + 4) >= expected {
                    break;
                }
            }
        }

        *captured_in_task.lock().await = String::from_utf8_lossy(&data).to_string();

        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status,
            if status == 200 { "OK" } else { "ERROR" },
            response_body.len(),
            response_body
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.flush().await;
    });

    (format!("http://{}", addr), captured)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[tokio::test]
async fn http_executor_posts_file_body_with_env_header() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");

    let (base_url, captured) = spawn_mock_server(200, "{}").await;

    let body_dir = tempfile::tempdir().expect("temp dir");
    let body_path = body_dir.path().join("app_settings.json");
    std::fs::write(
        &body_path,
        "{\"feature_flags\":{\"new_checkout\":true}}",
    )
    .expect("write body");

    std::env::set_var("STARTUP_TASK_SDK_TEST_TOKEN", "s3cr3t");
    let yaml = format!(
        "task_id: config.publish-app-settings\n\
         version: \"1\"\n\
         timeout_seconds: 10\n\
         execution:\n\
         \x20 executor: http\n\
         \x20 config:\n\
         \x20   method: put\n\
         \x20   url: {}/api/v1/apps/demo-app/settings\n\
         \x20   headers:\n\
         \x20     Authorization: Bearer ${{STARTUP_TASK_SDK_TEST_TOKEN}}\n\
         \x20     Content-Type: application/json\n\
         \x20   body_source: file\n\
         \x20   body_path: {}\n",
        base_url,
        body_path.display()
    );
    let spec = load_spec(&yaml).expect("spec should be valid");

    let result = builder.run(&spec).await;
    std::env::remove_var("STARTUP_TASK_SDK_TEST_TOKEN");

    assert_eq!(result.outcome, Outcome::Executed, "error = {:?}", result.error);
    assert_eq!(
        version_status(&pool, "config.publish-app-settings", "1")
            .await
            .as_deref(),
        Some("success")
    );

    // Wait for the mock server to finish reading the request before asserting.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let request = captured.lock().await.clone();
    let lower = request.to_lowercase();
    assert!(
        request.starts_with("PUT /api/v1/apps/demo-app/settings "),
        "request = {}",
        request
    );
    assert!(
        lower.contains("authorization: bearer s3cr3t"),
        "request = {}",
        request
    );
    assert!(
        request.contains("{\"feature_flags\":{\"new_checkout\":true}}"),
        "request = {}",
        request
    );

    pool.close().await;
}

#[tokio::test]
async fn http_unexpected_status_reports_contract_message() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let builder = Coordinator::new(store as Arc<dyn Store>, "pod-a");

    let (base_url, _captured) = spawn_mock_server(500, "boom").await;
    let yaml = format!(
        "task_id: config.publish-app-settings\n\
         version: \"3\"\n\
         timeout_seconds: 10\n\
         execution:\n\
         \x20 executor: http\n\
         \x20 config:\n\
         \x20   method: POST\n\
         \x20   url: {}/api/v1/apps/demo-app/settings\n\
         \x20   body_source: inline\n\
         \x20   body: '{{\"a\":1}}'\n",
        base_url
    );
    let spec = load_spec(&yaml).expect("spec should be valid");

    let result = builder.run(&spec).await;
    assert_eq!(result.outcome, Outcome::Failed);
    assert_eq!(
        result.error.as_deref(),
        Some("http status 500 not in [200] (body: boom)")
    );
    assert_eq!(
        version_status(&pool, "config.publish-app-settings", "3")
            .await
            .as_deref(),
        Some("failed")
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Extra: step-3 double check (the previous owner already recorded success during a takeover)
// ---------------------------------------------------------------------------

/// Wraps a real SQLite store and writes a success record right after a successful acquire,
/// simulating "the previous owner finished while we acquired the lock", which deterministically hits contract §5 step 3.
struct DoubleCheckRaceStore {
    inner: Arc<SqliteStore>,
    task_id: String,
    version: String,
    injected: AtomicUsize,
}

#[async_trait::async_trait]
impl Store for DoubleCheckRaceStore {
    async fn version_succeeded(
        &self,
        task_id: &str,
        version: &str,
    ) -> Result<bool, startup_task_sdk::StoreError> {
        self.inner.version_succeeded(task_id, version).await
    }

    async fn try_acquire(
        &self,
        task_id: &str,
        owner: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, startup_task_sdk::StoreError> {
        let acquired = self
            .inner
            .try_acquire(task_id, owner, now, expires_at)
            .await?;
        if acquired {
            self.inner
                .record_version(&self.task_id, &self.version, "success", now, "old-owner")
                .await?;
            self.injected.fetch_add(1, Ordering::SeqCst);
        }
        Ok(acquired)
    }

    async fn record_version(
        &self,
        task_id: &str,
        version: &str,
        status: &str,
        executed_at: DateTime<Utc>,
        owner: &str,
    ) -> Result<(), startup_task_sdk::StoreError> {
        self.inner
            .record_version(task_id, version, status, executed_at, owner)
            .await
    }

    async fn release(&self, task_id: &str, owner: &str) -> Result<(), startup_task_sdk::StoreError> {
        self.inner.release(task_id, owner).await
    }

    async fn ensure_schema(&self) -> Result<(), startup_task_sdk::StoreError> {
        self.inner.ensure_schema().await
    }

    fn dialect(&self) -> startup_task_sdk::Dialect {
        self.inner.dialect()
    }
}

#[tokio::test]
async fn double_check_returns_skipped_already_done_and_releases_lock() {
    let (_dir, pool) = setup_db().await;
    let inner = sqlite_store(&pool);

    let racing = Arc::new(DoubleCheckRaceStore {
        inner: inner.clone(),
        task_id: "cluster.publish-config".to_string(),
        version: "11".to_string(),
        injected: AtomicUsize::new(0),
    });

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut coordinator = Coordinator::new(racing.clone() as Arc<dyn Store>, "pod-new");
    coordinator
        .register_handler(
            "publish_config",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .expect("register handler");

    let spec = custom_spec("cluster.publish-config", "11", "publish_config");
    let result = coordinator.run(&spec).await;

    assert_eq!(racing.injected.load(Ordering::SeqCst), 1, "the record must be injected exactly once after the acquire");
    assert_eq!(
        result.outcome,
        Outcome::SkippedAlreadyDone,
        "a step-3 hit must return SKIPPED_ALREADY_DONE, error = {:?}",
        result.error
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0, "the handler must not run");
    assert_eq!(
        lock_count(&pool).await,
        0,
        "the early return at step 3 must still release our own lock (contract §5 point 5)"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Extra: injectable clock (expiry boundary + duration)
// ---------------------------------------------------------------------------

/// Manually advanced clock: `now()` and `monotonic()` share one offset.
#[derive(Debug)]
struct ManualClock {
    base_now: DateTime<Utc>,
    base_monotonic: Instant,
    offset_ms: AtomicI64,
}

impl ManualClock {
    fn new() -> Self {
        let base_now = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("parse base time")
            .with_timezone(&Utc);
        Self {
            base_now,
            base_monotonic: Instant::now(),
            offset_ms: AtomicI64::new(0),
        }
    }

    fn advance(&self, millis: i64) {
        self.offset_ms.fetch_add(millis, Ordering::SeqCst);
    }

    fn offset(&self) -> i64 {
        self.offset_ms.load(Ordering::SeqCst)
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        self.base_now + chrono::Duration::milliseconds(self.offset())
    }

    fn monotonic(&self) -> Instant {
        self.base_monotonic + Duration::from_millis(self.offset().max(0) as u64)
    }
}

#[tokio::test]
async fn clock_drives_expiry_boundary() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let clock = Arc::new(ManualClock::new());
    let task_id = "cluster.clock-boundary";

    // The lock's expires_at equals the clock's now: Q3 uses `expires_at < now`, so equality is not expired.
    insert_lock(
        &pool,
        task_id,
        "pod-old",
        clock.now() - chrono::Duration::seconds(30),
        clock.now(),
    )
    .await;

    let mut coordinator =
        Coordinator::with_clock(store as Arc<dyn Store>, "pod-new", clock.clone());
    coordinator
        .register_handler(
            "noop",
            Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> { Ok(()) }),
        )
        .expect("register handler");

    let spec = custom_spec(task_id, "1", "noop");

    let blocked = coordinator.run(&spec).await;
    assert_eq!(
        blocked.outcome,
        Outcome::SkippedLocked,
        "expires_at == now is not expired, so no takeover"
    );
    assert_eq!(
        lock_owner(&pool, task_id).await.as_deref(),
        Some("pod-old"),
        "a live lock must not be stolen"
    );

    // Advance one second: strictly expired, so we take over and execute.
    clock.advance(1000);
    let taken = coordinator.run(&spec).await;
    assert_eq!(taken.outcome, Outcome::Executed, "error = {:?}", taken.error);
    assert_eq!(lock_count(&pool).await, 0);
    assert_eq!(
        version_status(&pool, task_id, "1").await.as_deref(),
        Some("success")
    );

    pool.close().await;
}

#[tokio::test]
async fn clock_controls_duration() {
    let (_dir, pool) = setup_db().await;
    let store = sqlite_store(&pool);
    let clock = Arc::new(ManualClock::new());

    let mut coordinator =
        Coordinator::with_clock(store as Arc<dyn Store>, "pod-a", clock.clone());
    let clock_in_handler = clock.clone();
    coordinator
        .register_handler(
            "slow",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                clock_in_handler.advance(1500);
                Ok(())
            }),
        )
        .expect("register handler");

    let result = coordinator
        .run(&custom_spec("task.clock-duration", "1", "slow"))
        .await;

    assert_eq!(result.outcome, Outcome::Executed, "error = {:?}", result.error);
    assert_eq!(
        result.duration_ms(),
        1500,
        "duration comes from the injected monotonic clock and can be asserted exactly"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Extra: raw pass-through of acquire/version-check errors, the MySQL store, the process-wide owner_id
// ---------------------------------------------------------------------------

/// Decorator that fails only on Q1, used to verify "§7: FAILED always writes a failed record" and raw error pass-through.
struct FailingVersionCheckStore {
    inner: Arc<SqliteStore>,
}

#[async_trait::async_trait]
impl Store for FailingVersionCheckStore {
    async fn version_succeeded(
        &self,
        _task_id: &str,
        _version: &str,
    ) -> Result<bool, startup_task_sdk::StoreError> {
        Err(startup_task_sdk::StoreError::new("simulated q1 failure"))
    }

    async fn try_acquire(
        &self,
        task_id: &str,
        owner: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, startup_task_sdk::StoreError> {
        self.inner.try_acquire(task_id, owner, now, expires_at).await
    }

    async fn record_version(
        &self,
        task_id: &str,
        version: &str,
        status: &str,
        executed_at: DateTime<Utc>,
        owner: &str,
    ) -> Result<(), startup_task_sdk::StoreError> {
        self.inner
            .record_version(task_id, version, status, executed_at, owner)
            .await
    }

    async fn release(&self, task_id: &str, owner: &str) -> Result<(), startup_task_sdk::StoreError> {
        self.inner.release(task_id, owner).await
    }

    async fn ensure_schema(&self) -> Result<(), startup_task_sdk::StoreError> {
        self.inner.ensure_schema().await
    }

    fn dialect(&self) -> startup_task_sdk::Dialect {
        self.inner.dialect()
    }
}

#[tokio::test]
async fn version_check_error_is_passed_through_and_records_failed() {
    let (_dir, pool) = setup_db().await;
    let inner = sqlite_store(&pool);
    let failing = Arc::new(FailingVersionCheckStore { inner });

    let coordinator = Coordinator::new(failing as Arc<dyn Store>, "pod-a");
    let spec = custom_spec("cache/warm-up", "q1", "unused");

    let result = coordinator.run(&spec).await;

    assert_eq!(result.outcome, Outcome::Failed);
    assert_eq!(
        result.error.as_deref(),
        Some("simulated q1 failure"),
        "store errors must be passed through verbatim, with no prefix"
    );
    // Contract §7: FAILED always tries to write a failed record (a step-1 Q1 error counts too).
    assert_eq!(
        version_status(&pool, "cache/warm-up", "q1")
            .await
            .as_deref(),
        Some("failed")
    );
    assert_eq!(lock_count(&pool).await, 0, "without a lock we release nothing and leave nothing behind");

    pool.close().await;
}

#[tokio::test]
async fn mysql_store_reports_mysql_dialect() {
    use startup_task_sdk::{Dialect, MysqlStore};

    // Lazy pool: no network connection, we only check the structure and dialect tag.
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .connect_lazy("mysql://user:pwd@127.0.0.1:3306/app")
        .expect("lazy mysql pool");
    let store = MysqlStore::new(pool);

    assert_eq!(store.dialect(), Dialect::Mysql);
    assert_eq!(store.dialect().as_str(), "mysql");
}

#[test]
fn default_owner_id_is_stable_within_process() {
    let first = startup_task_sdk::default_owner_id();
    let second = startup_task_sdk::default_owner_id();

    assert_eq!(first, second, "owner_id must be stable within the process");
    let (host, suffix) = first.rsplit_once('/').expect("<hostname>/<uuid8>");
    assert!(!host.is_empty(), "hostname must not be empty: {}", first);
    assert_eq!(suffix.len(), 8, "uuid8 must be eight hex characters: {}", first);
    assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
}
