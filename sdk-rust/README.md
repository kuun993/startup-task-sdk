Chinese: [README.zh-CN.md](README.zh-CN.md)

# startup-task-sdk (Rust)


Startup task coordination SDK for Kubernetes pods that boot in parallel. A given `(task_id, version)`
must succeed at most once:

```
check version (Q1) → acquire the lock (Q2 atomic INSERT / Q3 expired-lock takeover) → double check (Q1)
→ execute (http / custom) → record the result (Q4 upsert) → release the lock (Q5, conditioned on owner_id)
```

The normative behaviour is `docs/design.md` at the repository root. This implementation follows it
point by point and trims nothing.

## Layout

| Path | Description |
|---|---|
| `src/spec.rs` | `TaskSpec`, `load_spec()`, `SpecError` (YAML parsing plus manual validation; messages carry the field path) |
| `src/store.rs` | `Store` trait, `PostgresStore`, `MysqlStore`, `SqliteStore`, dialect SQL and duplicate detection |
| `src/executor.rs` | http executor (reqwest), `${VAR}` substitution, error messages |
| `src/coordinator.rs` | `Coordinator`, the coordination algorithm, `Clock`, lock-release cleanup, panic convergence |
| `src/outcome.rs` | `Outcome`, `RunResult` |
| `src/schema.rs` | `ensure_schema(&store)` |
| `ddl/{sqlite,postgres,mysql}.sql` | Table DDL (identical to `spec/ddl/*.sql`) |
| `examples/` | Runnable http / custom examples |
| `tests/coordinator.rs` | The six scenarios of contract §10 (real SQLite SQL) plus error-convergence, clock and panic cases |
| `tests/spec.rs` | Negative spec-validation cases, environment variable substitution |

## Initialization

```rust
use std::sync::Arc;
use startup_task_sdk::{ensure_schema, Coordinator, MysqlStore, PostgresStore, SqliteStore, Store};

// PostgreSQL
let pg = PostgresStore::connect("postgres://user:pwd@127.0.0.1:5432/app").await?;
ensure_schema(&pg).await?;
let mut coordinator = Coordinator::new(Arc::new(pg) as Arc<dyn Store>, "pod-7f9c");

// MySQL
let mysql = MysqlStore::connect("mysql://user:pwd@127.0.0.1:3306/app").await?;
ensure_schema(&mysql).await?;
let mut coordinator = Coordinator::new(Arc::new(mysql) as Arc<dyn Store>, "pod-7f9c");

// Or SQLite (tests and embedded use only)
let sqlite = SqliteStore::connect("sqlite://startup-task.db?mode=rwc").await?;
ensure_schema(&sqlite).await?;
let mut coordinator = Coordinator::new(Arc::new(sqlite) as Arc<dyn Store>, "pod-7f9c");
```

Use the pod name as `owner_id` when you care about it. Otherwise call
`Coordinator::with_generated_owner(store)`, which produces `<hostname>/<uuid8>` and **keeps it fixed
for the lifetime of the process** (generated once and cached so Q5 always matches our own
acquisition).

`PostgresStore::new(PgPool)`, `MysqlStore::new(MySqlPool)` and `SqliteStore::new(SqlitePool)` accept
a pool the caller already owns. To customise SQLite options such as `journal_mode=WAL` and
`busy_timeout`, use `SqliteStore::connect_with_options(SqliteConnectOptions::new()...)`.

### Injecting a clock

```rust
use startup_task_sdk::{Clock, Coordinator};
use chrono::{DateTime, Utc};
use std::time::Instant;

struct FixedClock { now: DateTime<Utc>, mono: Instant }
impl Clock for FixedClock {
    fn now(&self) -> DateTime<Utc> { self.now }
    fn monotonic(&self) -> Instant { self.mono }
}

let coordinator = Coordinator::with_clock(
    store,
    "pod-7f9c",
    std::sync::Arc::new(FixedClock { now: Utc::now(), mono: Instant::now() }),
);
```

Every timestamp written to the database (`acquired_at`, `expires_at`, `executed_at`) and
`RunResult.duration` come from this clock, so tests can assert expiry boundaries and durations
exactly.

## Registering a custom handler

```rust
coordinator.register_handler(
    "warm_up_cache",
    Box::new(|config: serde_json::Value| -> Result<(), startup_task_sdk::HandlerError> {
        let keys = config["keys"].as_array().cloned().unwrap_or_default();
        let ttl_seconds = config["ttl_seconds"].as_u64().unwrap_or(300);
        // ... actually warm up these keys, using ttl_seconds as the TTL ...
        Ok(())
    }),
)?;
```

- The handler receives `execution.config` (empty `{}` by default, deep-copied, without the `handler` field).
- Returning `Ok(())` means success; returning `Err` means failure (`RunResult.error` takes the error's `Display`).
- **A `panic!` inside a handler never escapes `run()`**: `catch_unwind` turns it into `FAILED` with
  `error = 'handler panicked: <msg>'`, and the lock is released and a `failed` record written just the same.
- Registering the same name twice returns `Err(RegisterHandlerError)`.
- An unregistered handler yields `FAILED` with `error = 'handler "xxx" is not registered'`.

## Running

```rust
let spec = load_spec("examples/warm_up_cache.yaml")?;   // file path
let result = coordinator.run(&spec).await;
// or from YAML text: load_spec(yaml_str)
println!("{} {} {:?} {:?}", result.outcome.as_str(), result.duration_ms(), result.error, result.task_id);

let results = coordinator.run_all(&specs).await;         // serial, same order as the input
```

`run()` never panics and never returns `Err`; every failure converges into `Outcome::Failed` plus
`RunResult.error`. `Outcome::as_str()` / `Display` yield the upper-case constant names:
`EXECUTED`, `SKIPPED_ALREADY_DONE`, `SKIPPED_LOCKED`, `FAILED`.

## YAML fields

| Field | Required | Description |
|---|---|---|
| `task_id` | yes | 1..128 characters, matching `^[A-Za-z0-9._:/-]+$` |
| `version` | yes | 1..64 characters; a change means a new batch |
| `description` | no | Free-form description |
| `timeout_seconds` | no | Defaults to 300 and must be > 0; also the HTTP request timeout and the lock's `expires_at` offset |
| `execution.executor` | yes | `http` / `custom` (case sensitive) |

### `executor: http`

| Field | Required | Description |
|---|---|---|
| `config.method` | yes | Upper-cased before being handed to the HTTP client |
| `config.url` | yes | Supports `${ENV}` |
| `config.headers` | no | string→string; values support `${ENV}` |
| `config.body_source` | no | `file` / `inline` / `none` (default `none`) |
| `config.body_path` | when `body_source=file` | Relative paths resolve against the spec file directory when the spec came from a file, else against the process working directory |
| `config.body` | no | Request body for `body_source=inline`, supports `${ENV}` |
| `config.success_check.http_status` | no | int or list of ints, default `200` |

### `executor: custom`

`execution.handler` is required (non-empty); `execution.config` is optional and passed to the handler
unchanged.

### Environment variable substitution

Applies only to `url`, header values and the inline `body`, and happens **before the HTTP request**
(not at parse time); `body_path` is not substituted. `NAME` inside `${NAME}` must match
`[A-Za-z_][A-Za-z0-9_]*`; anything else is left as-is.

### Fixed error messages

| Situation | `RunResult.error` |
|---|---|
| Missing environment variable | `environment variable "NAME" is not set` |
| Unexpected status code | `http status 500 not in [200] (body: ...)` |
| Transport error / timeout | `http request failed: <reason>` |
| Handler not registered | `handler "xxx" is not registered` |
| Handler panic | `handler panicked: <panic message>` |

Storage errors (SQL errors from Q1..Q5) are **passed through verbatim** to `RunResult.error`, with no
prefix added.

## Examples

`examples/` holds two runnable scenarios (identical file names and content across the four
languages):

| File | Description |
|---|---|
| `publish_app_settings.yaml` | `executor: http`: `PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings`, header with `${CONFIG_CENTER_TOKEN}`, body from `app_settings.json` |
| `app_settings.json` | Request body of the task above (feature flags plus limits) |
| `warm_up_cache.yaml` | `executor: custom`: `handler: warm_up_cache` with `keys` and `ttl_seconds` in its config |
| `run_examples.rs` | Registers the handler, creates a SQLite database and `run_all`s both tasks |

```bash
cd sdk-rust
cargo run -q --example run_examples
```

The example uses SQLite (default `%TEMP%/startup-task-sdk-example.db`, override with
`STARTUP_TASK_DB`). After registering `warm_up_cache` it `run_all`s both tasks: when the config
center is unreachable the http task prints `FAILED` (no panic) and the custom task prints
`EXECUTED`; running it again turns the custom task into `SKIPPED_ALREADY_DONE`.

```text
store      : sqlite://.../startup-task-sdk-example.db?mode=rwc

== run_all (2 tasks) ==
    warm_up_cache: warming up 3 keys ["home:feed", "home:recommend", "config:feature-flags"], ttl_seconds=300
  FAILED task_id=config/publish-app-settings version=2026-10-22-v1 duration_ms=37 error=<why the config center was unreachable or returned a non-2xx status>
  EXECUTED task_id=cache/warm-up version=2026-10-22-v1 duration_ms=24
```

When `CONFIG_CENTER_URL` / `CONFIG_CENTER_TOKEN` / `APP_NAME` are unset the example fills in demo
defaults (`http://127.0.0.1:8080` / `example-token` / `demo-app`).

## Tests

```bash
cd sdk-rust
cargo test
```

- `tests/coordinator.rs`: the six scenarios of contract §10 (`first_run_executed`,
  `second_run_skipped_already_done`, `concurrent_only_one_executes`, `expired_lock_takeover`,
  `failure_releases_lock_and_no_success`, `release_does_not_delete_other_owner_lock`) plus
  error-convergence cases (unregistered/duplicate handler, handler panic, a `failed` record allowing
  a retry whose success overwrites it, `run_all` ordering and isolation, http missing env var /
  transport error / unexpected status code, a step-3 double-check hit, raw pass-through of Q1
  errors). Every test uses its own temporary database file with `journal_mode=WAL` +
  `busy_timeout=10s` + `max_connections=4`; the concurrency case runs **eight independent
  Coordinators** (each with its own owner_id, sharing a real multi-connection pool); the http
  executor is exercised against a local mock server; two clock cases use an injected `ManualClock`
  to assert the expiry boundary (`expires_at == now` is not expired) and `duration` exactly.
- `tests/spec.rs`: negative spec-validation cases (including field paths for unknown fields),
  method upper-casing, environment variable substitution.
- `tests/integration_pg.rs` / `tests/integration_mysql.rs`: **real-database integration tests** (next section).
- Everything runs without Docker or a local database: unit and contract tests all use the SQLite
  dialect with real SQL, and the integration tests skip themselves when no DSN is set (they print one
  line and return, still counting as passed).

## Integration tests (real databases)

The real-database tests need two DSN environment variables; credentials are never stored in the
repository:

```bash
cd sdk-rust
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
cargo test                              # with the DSNs: runs the real-database cases
cargo test --test integration_pg        # PostgreSQL only
cargo test --test integration_mysql     # MySQL / MariaDB only
```

**Skip mechanism**: Rust test functions have no built-in skip. Each integration test reads its DSN
first; when it is missing the test prints
`skipping ...: STARTUP_TASK_*_DSN is not set` and returns, so `cargo test` stays green on machines
without a database (nine cases per file, a few milliseconds when skipped).

Conventions:

- Tables are created only through `ensure_schema` and **no database is ever dropped**. Every case
  uses a `task_id` with a unique process-id plus nanosecond suffix (`itest.pg.*` /
  `itest.mysql.*`) and `DELETE`s its own two rows at the end, so cases do not interfere and can run
  in parallel (a failing assertion leaves that case's rows behind for debugging; a successful run
  leaves zero rows).
- Each case builds its pool inside its own Tokio runtime (a `static` pool would be reused across
  runtimes and fail with `A Tokio 1.x context was found, but it is being shutdown`); normal cases use
  `max_connections = 2`, the concurrency case uses `= 8` after warming up eight connections so
  TCP/auth jitter stays outside the timed window.
- Concurrency determinism: the handler holds the lock with
  **`tokio::task::block_in_place`** until the other seven pods finish. A plain `std::thread::sleep`
  would block that worker (which may own the I/O driver) and stall every other task — measured as a
  consistent 30s hang. The same holds for users: block with `block_in_place` in a synchronous
  handler.
- `CREATE TABLE IF NOT EXISTS` races on PostgreSQL, so schema creation is serialised with a
  process-wide async lock.
- PostgreSQL and MySQL time columns have microsecond precision: the tests truncate `Utc::now()` to
  microseconds before asserting "read back == sent" (SDK behaviour is unchanged and the `expires_at`
  comparison is unaffected).

| Coverage | PG | MySQL |
|---|---|---|
| `ensure_schema` creates the tables with the right column types (PG `VARCHAR(128)`/`TIMESTAMP`, MySQL `VARCHAR(128)`/`DATETIME(6)`) and is idempotent | ✓ | ✓ |
| First run `EXECUTED` + `success` + lock table emptied | ✓ | ✓ |
| Same version again is `SKIPPED_ALREADY_DONE`, lock table empty before and after | ✓ | ✓ |
| Eight concurrent runs → exactly one `EXECUTED` / seven `SKIPPED_LOCKED` / handler called once | ✓ | ✓ |
| Expired lock taken over; the handler asserts the lock owner is already the new pod | ✓ | ✓ |
| Handler `Err` → `FAILED` + lock released + `failed` record | ✓ | ✓ |
| A acquires, the lock is reassigned to B, A's release leaves B's lock alone | ✓ | ✓ |
| A live lock takes the "lock taken" branch (proving `23505` / `1062` are detected) | ✓ | ✓ |
| Time binding neither drifts on read-back nor on comparison under session time zone `Asia/Shanghai` | ✓ | ✓ (`+08:00`) |
| `ON DUPLICATE KEY UPDATE` keeps one row and overwrites it (failed → success) | — | ✓ |

## Known trade-offs

1. **A takeover can execute a task twice.** Once a lock expires (`expires_at < now`) any pod may take
   it over while the previous owner may still be running, so the same `(task_id, version)` can be
   executed twice. Contract §5.3 accepts this; non-idempotent tasks must make themselves safe, or
   `timeout_seconds` must be larger than the worst-case runtime.
2. **Only `status = 'success'` de-duplicates.** A `failed` record does not block a retry; rerunning a
   failed task overwrites the old record (Q4 upsert).
3. **A failed release is only logged.** It does not change `Outcome`; the lock expires naturally after
   `expires_at` and can then be taken over.
4. **Only database errors from the acquire phase (Q2/Q3) skip the version record and the release**
   (contract §7): the lock state cannot be determined safely at that point. A Q1 error in steps 1/3
   and any execution-phase error always write a `status='failed'` record.
5. **MySQL duplicate detection only accepts vendor code `1062`** (contract §4.3); SQLite accepts the
   extended codes `1555`/`2067` with SQLState `23505` as a fallback; PostgreSQL only accepts SQLState
   `23505`. There is no "guess from the error text" fallback.
6. **Handlers are synchronous functions** called inline from an async task; a handler panic is
   converged into `FAILED` by `catch_unwind` (writing a `failed` record and releasing the lock).
   When you really need to block (sleep, waiting on a lock, heavy CPU), use
   `tokio::task::block_in_place(|| ...)` (multi-thread runtime) or hand the work to `spawn_blocking`:
   a bare `std::thread::sleep` blocks the worker and, in the worst case, the runtime's I/O driver,
   stalling every other task in the process.
7. **SQLite stores time as TEXT** (`YYYY-MM-DD HH:MM:SS.ffffff`, UTC, six fractional digits), so
   lexicographic order equals chronological order; all times are UTC with no local time zone
   conversion.
8. **`timeout_seconds` is clamped to 100 years** when computing `expires_at`, so extreme values cannot
   overflow `chrono` (`run` must not panic); validation semantics are unchanged (still only `> 0`).
