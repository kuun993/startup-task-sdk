//! Real-database integration tests for PostgreSQL.
//!
//! Requires the `STARTUP_TASK_PG_DSN` environment variable; without it each test prints one skip line and returns,
//! so `cargo test` stays green on machines without a database.
//!
//! ```bash
//! cd sdk-rust
//! export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
//! cargo test --test integration_pg
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use startup_task_sdk::{
    ensure_schema, load_spec, Coordinator, HandlerError, Outcome, PostgresStore, Store, TaskSpec,
};

const SKIP_HINT: &str = "skipping postgres integration tests: STARTUP_TASK_PG_DSN is not set";

fn dsn() -> Option<String> {
    match std::env::var("STARTUP_TASK_PG_DSN") {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => {
            eprintln!("{}", SKIP_HINT);
            None
        }
    }
}

/// Each test builds its pool inside its own Tokio runtime: a `static` pool would be reused across runtimes
/// and fail with `A Tokio 1.x context was found, but it is being shutdown` once a runtime ends.
async fn connect(dsn: &str) -> PgPool {
    connect_sized(dsn, 2).await
}

async fn connect_sized(dsn: &str, max_connections: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(30))
        .connect(dsn)
        .await
        .expect("connect to postgres failed (check STARTUP_TASK_PG_DSN)")
}

/// Run one session statement on every new connection (for example `SET TIME ZONE 'Asia/Shanghai'`),
/// used to confirm time binding/comparison does not drift with the session time zone.
async fn connect_with_session_statement(dsn: &str, statement: &'static str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(15))
        .after_connect(move |conn, _metadata| {
            Box::pin(async move {
                sqlx::query(statement).execute(conn).await?;
                Ok(())
            })
        })
        .connect(dsn)
        .await
        .expect("connect to postgres (with session setup) failed")
}

/// Concurrent `CREATE TABLE IF NOT EXISTS` races on PostgreSQL, so serialise schema creation.
static SCHEMA_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn ensure_schema_serialized(store: &dyn Store) {
    let _guard = SCHEMA_LOCK.lock().await;
    ensure_schema(store).await.expect("ensure_schema on postgres");
}

fn unique_task(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("itest.pg.{}.{}.{}", name, std::process::id(), nanos)
}

/// PostgreSQL `TIMESTAMP` only keeps microseconds while `Utc::now()` carries nanoseconds:
/// truncate the value we send to microseconds before asserting "read back == sent".
fn truncate_to_micros(value: DateTime<Utc>) -> DateTime<Utc> {
    use chrono::Timelike;
    let nanos = value.timestamp_subsec_nanos();
    value
        .with_nanosecond(nanos - nanos % 1_000)
        .expect("truncate to microsecond precision")
}

fn custom_spec(task_id: &str, version: &str, handler: &str) -> TaskSpec {
    let yaml = format!(
        "task_id: {}\nversion: \"{}\"\ntimeout_seconds: 30\nexecution:\n  executor: custom\n  handler: {}\n",
        task_id, version, handler
    );
    load_spec(&yaml).expect("spec should be valid")
}

async fn cleanup(pool: &PgPool, task_id: &str) {
    for statement in [
        "DELETE FROM task_execution_lock WHERE task_id = $1",
        "DELETE FROM task_version_record WHERE task_id = $1",
    ] {
        sqlx::query(statement)
            .bind(task_id)
            .execute(pool)
            .await
            .expect("cleanup");
    }
}

async fn version_status(pool: &PgPool, task_id: &str, version: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT status FROM task_version_record WHERE task_id = $1 AND version = $2",
    )
    .bind(task_id)
    .bind(version)
    .fetch_optional(pool)
    .await
    .expect("select version status")
}

async fn version_count(pool: &PgPool, task_id: &str, version: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM task_version_record WHERE task_id = $1 AND version = $2",
    )
    .bind(task_id)
    .bind(version)
    .fetch_one(pool)
    .await
    .expect("count version records")
}

async fn lock_rows(pool: &PgPool, task_id: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM task_execution_lock WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("count lock rows")
}

async fn lock_owner(pool: &PgPool, task_id: &str) -> Option<String> {
    sqlx::query_scalar::<_, String>("SELECT owner_id FROM task_execution_lock WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("select lock owner")
}

async fn insert_lock(
    pool: &PgPool,
    task_id: &str,
    owner: &str,
    acquired_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) {
    sqlx::query(
        "INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(task_id)
    .bind(owner)
    .bind(acquired_at.naive_utc())
    .bind(expires_at.naive_utc())
    .execute(pool)
    .await
    .expect("insert lock");
}

#[tokio::test]
async fn ensure_schema_creates_expected_tables() {
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect(&dsn).await;
    let store = PostgresStore::new(pool.clone());

    ensure_schema_serialized(&store).await;
    // Idempotent: running it again must not fail.
    ensure_schema(&store).await.expect("ensure_schema must be idempotent");

    let rows = sqlx::query(
        "SELECT table_name, column_name, data_type, character_maximum_length \
           FROM information_schema.columns \
          WHERE table_name IN ('task_execution_lock', 'task_version_record') \
            AND table_schema = current_schema()",
    )
    .fetch_all(&pool)
    .await
    .expect("inspect columns");

    let mut columns: Vec<(String, String, String, Option<i32>)> = Vec::new();
    for row in &rows {
        columns.push((
            row.try_get("table_name").expect("table_name"),
            row.try_get("column_name").expect("column_name"),
            row.try_get("data_type").expect("data_type"),
            row.try_get("character_maximum_length").ok().flatten(),
        ));
    }

    let find = |table: &str, column: &str| {
        columns
            .iter()
            .find(|(t, c, _, _)| t == table && c == column)
            .unwrap_or_else(|| panic!("missing column {}.{}; got {:?}", table, column, columns))
    };

    let (_, _, task_id_type, task_id_len) = find("task_execution_lock", "task_id");
    assert_eq!(task_id_type, "character varying");
    assert_eq!(*task_id_len, Some(128), "PG task_id must be VARCHAR(128)");

    let (_, _, acquired_type, _) = find("task_execution_lock", "acquired_at");
    assert_eq!(
        acquired_type, "timestamp without time zone",
        "PG time columns must be TIMESTAMP without time zone, bound as chrono::NaiveDateTime"
    );
    let (_, _, expires_type, _) = find("task_execution_lock", "expires_at");
    assert_eq!(expires_type, "timestamp without time zone");

    let (_, _, status_type, status_len) = find("task_version_record", "status");
    assert_eq!(status_type, "character varying");
    assert_eq!(*status_len, Some(16));
    let (_, _, executed_type, _) = find("task_version_record", "executed_at");
    assert_eq!(executed_type, "timestamp without time zone");

    pool.close().await;
}

#[tokio::test]
async fn first_run_executed_and_lock_released() {
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect(&dsn).await;
    let store = PostgresStore::new(pool.clone());
    ensure_schema_serialized(&store).await;

    let task = unique_task("first");
    cleanup(&pool, &task).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut coordinator = Coordinator::new(Arc::new(store) as Arc<dyn Store>, "pod-pg-a");
    coordinator
        .register_handler(
            "noop",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .expect("register handler");

    let spec = custom_spec(&task, "1", "noop");
    let result = coordinator.run(&spec).await;

    assert_eq!(result.outcome, Outcome::Executed, "error = {:?}", result.error);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(version_status(&pool, &task, "1").await.as_deref(), Some("success"));
    assert_eq!(lock_rows(&pool, &task).await, 0, "the lock must be released");

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[tokio::test]
async fn second_run_skipped_already_done() {
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect(&dsn).await;
    let store = PostgresStore::new(pool.clone());
    ensure_schema_serialized(&store).await;

    let task = unique_task("second");
    cleanup(&pool, &task).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let mut coordinator = Coordinator::new(Arc::new(store) as Arc<dyn Store>, "pod-pg-a");
    coordinator
        .register_handler(
            "noop",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .expect("register handler");

    let spec = custom_spec(&task, "1", "noop");
    assert_eq!(lock_rows(&pool, &task).await, 0, "the lock table is empty before the run");

    let first = coordinator.run(&spec).await;
    assert_eq!(first.outcome, Outcome::Executed, "error = {:?}", first.error);
    assert_eq!(lock_rows(&pool, &task).await, 0, "the lock table is empty after the run");

    let second = coordinator.run(&spec).await;
    assert_eq!(second.outcome, Outcome::SkippedAlreadyDone);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "no execution after a success record");
    assert_eq!(lock_rows(&pool, &task).await, 0, "the skip path writes no lock");

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn concurrent_only_one_executes() {
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect_sized(&dsn, 8).await;
    let store = PostgresStore::new(pool.clone());
    ensure_schema_serialized(&store).await;

    let task = unique_task("concurrent");
    cleanup(&pool, &task).await;

    // Warm up eight connections so TCP/auth jitter stays outside the timed window and no pod wastes time opening one.
    let mut warmup = Vec::with_capacity(8);
    for _ in 0..8 {
        warmup.push(pool.acquire().await.expect("warm up pool connections"));
    }
    drop(warmup);

    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let spec = Arc::new(custom_spec(&task, "1", "slow"));

    let mut handles = Vec::new();
    for index in 0..8 {
        let counter = calls.clone();
        let finished = completed.clone();
        let waiting = completed.clone();
        let mut coordinator = Coordinator::new(
            Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
            format!("pod-pg-{}", index),
        );
        coordinator
            .register_handler(
                "slow",
                Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                    counter.fetch_add(1, Ordering::SeqCst);
                    // Hold the lock until the other seven pods finish (they get SKIPPED_LOCKED),
                    // which makes the assertion independent of scheduling speed; 30s is only a defensive fuse.
                    // block_in_place tells the runtime that we block: it hands this worker's
                    // task queue and I/O driver to another worker. A bare std::thread::sleep
                    // would stall the scheduler too and the other seven pods would never finish.
                    tokio::task::block_in_place(|| {
                        let deadline = std::time::Instant::now() + Duration::from_secs(30);
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

    let executed = results.iter().filter(|r| r.outcome == Outcome::Executed).count();
    let locked = results
        .iter()
        .filter(|r| r.outcome == Outcome::SkippedLocked)
        .count();

    assert_eq!(executed, 1, "exactly one EXECUTED, actual: {:?}", results);
    assert_eq!(locked, 7, "all the others are SKIPPED_LOCKED, actual: {:?}", results);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(lock_rows(&pool, &task).await, 0);
    assert_eq!(version_status(&pool, &task, "1").await.as_deref(), Some("success"));

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[tokio::test]
async fn live_lock_by_other_owner_is_not_taken_over() {
    // Proves PG 23505 detection really takes the "lock taken" branch:
    // if detection failed, the INSERT error would surface as FAILED instead of SKIPPED_LOCKED.
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect(&dsn).await;
    let store = PostgresStore::new(pool.clone());
    ensure_schema_serialized(&store).await;

    let task = unique_task("live-lock");
    cleanup(&pool, &task).await;

    let now = Utc::now();
    insert_lock(&pool, &task, "pod-old", now, now + chrono::Duration::seconds(120)).await;

    let mut coordinator = Coordinator::new(Arc::new(store) as Arc<dyn Store>, "pod-new");
    coordinator
        .register_handler(
            "noop",
            Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> { Ok(()) }),
        )
        .expect("register handler");

    let result = coordinator.run(&custom_spec(&task, "1", "noop")).await;

    assert_eq!(
        result.outcome,
        Outcome::SkippedLocked,
        "a live lock must yield SKIPPED_LOCKED (23505 must be detected), error = {:?}",
        result.error
    );
    assert_eq!(lock_owner(&pool, &task).await.as_deref(), Some("pod-old"));

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_lock_takeover_owner_visible_in_handler() {
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect(&dsn).await;
    let store = PostgresStore::new(pool.clone());
    ensure_schema_serialized(&store).await;

    let task = unique_task("takeover");
    cleanup(&pool, &task).await;

    let now = Utc::now();
    insert_lock(
        &pool,
        &task,
        "pod-old",
        now - chrono::Duration::seconds(120),
        now - chrono::Duration::seconds(60),
    )
    .await;

    let observed: Arc<std::sync::Mutex<Option<Option<String>>>> =
        Arc::new(std::sync::Mutex::new(None));
    let observed_in_handler = observed.clone();
    let pool_in_handler = pool.clone();
    let task_in_handler = task.clone();

    let mut coordinator = Coordinator::new(Arc::new(store) as Arc<dyn Store>, "pod-new");
    coordinator
        .register_handler(
            "inspect",
            Box::new(move |_config: serde_json::Value| -> Result<(), HandlerError> {
                let handle = tokio::runtime::Handle::current();
                let pool = pool_in_handler.clone();
                let task = task_in_handler.clone();
                let owner = tokio::task::block_in_place(|| {
                    handle.block_on(async move { lock_owner(&pool, &task).await })
                });
                *observed_in_handler.lock().expect("observed") = Some(owner.clone());
                assert_eq!(
                    owner.as_deref(),
                    Some("pod-new"),
                    "the lock must belong to the taking-over pod during execution"
                );
                Ok(())
            }),
        )
        .expect("register handler");

    let result = coordinator.run(&custom_spec(&task, "1", "inspect")).await;

    assert_eq!(result.outcome, Outcome::Executed, "error = {:?}", result.error);
    assert_eq!(
        observed.lock().expect("observed").clone(),
        Some(Some("pod-new".to_string()))
    );
    assert_eq!(lock_rows(&pool, &task).await, 0, "the taken-over lock is released after execution");
    assert_eq!(version_status(&pool, &task, "1").await.as_deref(), Some("success"));

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[tokio::test]
async fn failure_releases_lock_and_records_failed() {
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect(&dsn).await;
    let store = PostgresStore::new(pool.clone());
    ensure_schema_serialized(&store).await;

    let task = unique_task("failure");
    cleanup(&pool, &task).await;

    let mut coordinator = Coordinator::new(Arc::new(store) as Arc<dyn Store>, "pod-pg-a");
    coordinator
        .register_handler(
            "fails",
            Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> {
                Err(Box::new(SimpleError("boom on postgres")))
            }),
        )
        .expect("register handler");

    let spec = custom_spec(&task, "1", "fails");

    let result = coordinator.run(&spec).await;
    assert_eq!(result.outcome, Outcome::Failed);
    assert!(result.error.as_deref().unwrap_or("").contains("boom on postgres"));
    assert_eq!(version_status(&pool, &task, "1").await.as_deref(), Some("failed"));
    assert_eq!(lock_rows(&pool, &task).await, 0, "the failure path must release the lock too");

    // Retry the same version: failed does not block it and success overwrites (Q4 upsert keeps a single row).
    let retry = coordinator.run(&spec).await;
    assert_eq!(retry.outcome, Outcome::Failed, "the handler always fails");
    assert_eq!(version_count(&pool, &task, "1").await, 1, "the upsert must not insert a second row");

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[tokio::test]
async fn release_does_not_delete_other_owner_lock() {
    let Some(dsn) = dsn() else {
        return;
    };
    let pool = connect(&dsn).await;
    let store = Arc::new(PostgresStore::new(pool.clone()));
    ensure_schema_serialized(store.as_ref()).await;

    let task = unique_task("release-other");
    cleanup(&pool, &task).await;

    let now = Utc::now();
    assert!(store
        .try_acquire(&task, "pod-a", now, now + chrono::Duration::seconds(60))
        .await
        .expect("acquire"));
    assert_eq!(lock_owner(&pool, &task).await.as_deref(), Some("pod-a"));

    // Simulate a takeover: reassign the lock row to B.
    sqlx::query("UPDATE task_execution_lock SET owner_id = $1 WHERE task_id = $2")
        .bind("pod-b")
        .bind(&task)
        .execute(&pool)
        .await
        .expect("simulate takeover");

    let coordinator_a = Coordinator::new(store as Arc<dyn Store>, "pod-a");
    coordinator_a
        .release_lock(&task)
        .await
        .expect("release must not error");

    assert_eq!(
        lock_owner(&pool, &task).await.as_deref(),
        Some("pod-b"),
        "A must not delete B's lock"
    );
    assert_eq!(lock_rows(&pool, &task).await, 1);

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[tokio::test]
async fn time_binding_roundtrip_under_shanghai_session() {
    let Some(dsn) = dsn() else {
        return;
    };
    // Every connection runs with session time zone Asia/Shanghai.
    let pool = connect_with_session_statement(&dsn, "SET TIME ZONE 'Asia/Shanghai'").await;
    let store = PostgresStore::new(pool.clone());
    ensure_schema_serialized(&store).await;

    let session_zone: String = sqlx::query_scalar("SHOW TIME ZONE")
        .fetch_one(&pool)
        .await
        .expect("show timezone");
    assert_eq!(session_zone, "Asia/Shanghai", "the session time zone must be in effect");

    let task = unique_task("tz");
    cleanup(&pool, &task).await;

    // Bind a UTC NaiveDateTime the way the SDK does and read it back unchanged.
    let now = truncate_to_micros(Utc::now());
    let expires_at = now + chrono::Duration::seconds(3600);
    assert!(store
        .try_acquire(&task, "pod-tz", now, expires_at)
        .await
        .expect("acquire"));

    let row = sqlx::query(
        "SELECT owner_id, acquired_at, expires_at FROM task_execution_lock WHERE task_id = $1",
    )
    .bind(&task)
    .fetch_one(&pool)
    .await
    .expect("select lock");

    let owner: String = row.try_get("owner_id").expect("owner_id");
    let acquired_at: NaiveDateTime = row.try_get("acquired_at").expect("acquired_at");
    let stored_expires_at: NaiveDateTime = row.try_get("expires_at").expect("expires_at");

    assert_eq!(owner, "pod-tz");
    assert_eq!(
        acquired_at, now.naive_utc(),
        "TIMESTAMP must read back as the UTC instant we sent (no drift under a +08:00 session)"
    );
    assert_eq!(stored_expires_at, expires_at.naive_utc());

    // Check again after converting to DateTime<Utc>.
    let acquired_utc: DateTime<Utc> = acquired_at.and_utc();
    assert_eq!(acquired_utc, now);
    assert_eq!(
        (stored_expires_at.and_utc() - now).num_seconds(),
        3600,
        "expires_at must be now + timeout_seconds"
    );

    // A live lock still must not be stolen under a +08:00 session.
    let mut coordinator = Coordinator::new(Arc::new(store) as Arc<dyn Store>, "pod-tz-2");
    coordinator
        .register_handler(
            "noop",
            Box::new(|_config: serde_json::Value| -> Result<(), HandlerError> { Ok(()) }),
        )
        .expect("register handler");
    let spec = custom_spec(&task, "1", "noop");
    let blocked = coordinator.run(&spec).await;
    assert_eq!(
        blocked.outcome,
        Outcome::SkippedLocked,
        "the session time zone must not affect the expires_at comparison, error = {:?}",
        blocked.error
    );

    // Mark the lock expired (still written as naive UTC); under +08:00 the expiry decision must be correct and allow takeover.
    sqlx::query("UPDATE task_execution_lock SET expires_at = $1 WHERE task_id = $2")
        .bind((now - chrono::Duration::seconds(60)).naive_utc())
        .bind(&task)
        .execute(&pool)
        .await
        .expect("expire lock");

    let taken = coordinator.run(&spec).await;
    assert_eq!(
        taken.outcome,
        Outcome::Executed,
        "the expiry decision must not depend on the session time zone, error = {:?}",
        taken.error
    );

    // executed_at must also be "now" in UTC, with no +8 hour shift.
    let executed_at: NaiveDateTime = sqlx::query_scalar(
        "SELECT executed_at FROM task_version_record WHERE task_id = $1 AND version = $2",
    )
    .bind(&task)
    .bind("1")
    .fetch_one(&pool)
    .await
    .expect("select executed_at");
    let drift = (executed_at.and_utc() - Utc::now()).num_seconds().abs();
    assert!(drift <= 60, "executed_at drifted by {} seconds, suspicious timezone issue", drift);
    assert_eq!(lock_rows(&pool, &task).await, 0);

    cleanup(&pool, &task).await;
    pool.close().await;
}

#[derive(Debug)]
struct SimpleError(&'static str);

impl std::fmt::Display for SimpleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for SimpleError {}
