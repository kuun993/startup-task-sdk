Chinese: [design.zh-CN.md](design.zh-CN.md)

# Startup task coordination SDK — overall design

> This document is the shared design baseline for the four language implementations (Python / Go /
> Java / Rust). Every behavioural rule here is **normative**: if an implementation conflicts with
> this document, this document wins and the other three languages are corrected as well.

## 1. Overview and goals

### 1.1 The problem

In a Kubernetes deployment the replicas start at the same time, and each of them has to run a batch
of initialization actions (publishing default configuration to the config center, warming hot
caches, syncing user permissions, and so on). Two hard constraints apply to such actions:

1. **They must not run concurrently**: two replicas publishing the same configuration at the same
   time overwrite each other and may even create duplicate resources;
2. **A given batch should succeed only once**: later restarts and rolling deployments must not run
   it again.

### 1.2 Approach

Collapse the "should this run?" coordination logic into one SDK; what runs is declared by the caller
in YAML:

```
check version → acquire the execution right → execute → record the result → release
```

Coordination state lives in the **application's own relational database** (PostgreSQL / MySQL, plus
SQLite for tests and embedding), with no Redis, ZooKeeper or separate lock service. The four SDKs
behave identically: algorithm steps, SQL semantics, result enum and error-convergence rules match
item by item.

### 1.3 Design principles

- **The database is the only source of coordination truth**: no in-process state, no filesystem, no
  network broadcast.
- **Atomicity is delegated to the database**: acquiring the lock has only two paths — a primary-key
  conflict and a matching conditional `UPDATE`; "check first, then insert" is forbidden.
- **Delete only your own lock**: release must carry `owner_id`, so a takeover owner's lock is never
  deleted by mistake.
- **Exceptions never swallow the lock**: every exit path (success, failure, early return, panic) must
  attempt to release the lock.
- **Behaviour reproducible across languages**: time, decisions, SQL and result strings must not
  depend on the language runtime or the session configuration.
- **Failures are diagnosable**: logging covers five event classes — acquired, takeover, skipped,
  failed, release failed.

### 1.4 Non-goals

No separate lock service; no other executors (shell / gRPC / database); no cron scheduling; no UI or
admin console; no absolute deduplication guarantee for non-idempotent operations under crash
takeover (see §11).

## 2. Architecture and public API

### 2.1 Runtime shape

```
Each pod starts in K8s
        │
        ├── application init (logging, config, data source)
        │
        ├── build Store (PG / MySQL / SQLite; caller supplies the connection)
        ├── build Coordinator (inject Store, owner_id, optional clock)
        ├── register_handler(...) × N   # needed by the custom executor
        └── run_all([spec...]) ──┐
                                 │
                ┌────────────────┴───────────────┐
                │          Coordinator           │ coordination algorithm (§5)
                └──────┬──────────────────┬──────┘
                       │                  │
                ┌──────┴──────┐   ┌───────┴───────┐
                │    Store    │   │   Executor    │
                │ dialect SQL │   │ http / custom │
                └──────┬──────┘   └───────────────┘
                       │
      application database (two tables)
```

Component responsibilities:

| Component | Responsibility | Not responsible for |
|---|---|---|
| `Coordinator` | Coordination flow, result convergence, lock release, logging | Task logic, connection pools |
| `Store` | The five parameterized SQL statements, dialect differences, time binding, conflict detection | Retries, transaction orchestration |
| `HttpExecutor` | Issue one HTTP request, decide on the status code, substitute environment variables | Retries, response body parsing |
| `Handler` (custom) | The caller-registered `name → fn` | The SDK caring about its internals |
| `TaskSpec` | Product of YAML parsing and validation (immutable) | Runtime state |

### 2.2 Public API

| Capability | Python | Java | Go | Rust |
|---|---|---|---|---|
| Construct | `Coordinator(store, owner_id=None, clock=None)` | `new Coordinator(store, ownerId[, clock])` | `NewCoordinator(store, ownerID)` / `NewCoordinatorWithClock` | `Coordinator::new(store, owner)` / `with_clock` |
| Register a handler | `register_handler(name, fn)` | `registerHandler(name, handler)` | `RegisterHandler(name, fn)` | `register_handler(name, f)` |
| Load a definition | `load_spec(text_or_path)` | `TaskSpecLoader.load(path_or_text)` | `LoadSpec(src)` | `load_spec(src)` |
| Single task | `run(spec)` | `run(spec)` | `Run(ctx, spec)` | `run(&spec).await` |
| Batch | `run_all(specs)` | `runAll(specs)` | `RunAll(ctx, specs)` | `run_all(&specs).await` |
| Create tables | `ensure_schema(store)` | `Schema.ensure(store)` | `EnsureSchema(ctx, store)` | `ensure_schema(&store).await` |

Conventions:

- Registering a duplicate `register_handler` name must fail (Python `ValueError` / Java
  `IllegalStateException` / Go `error` / Rust `Err`), so two different tasks cannot silently
  overwrite one name.
- `owner_id` is the executor identity (pod name or UUID). When not passed explicitly it defaults to
  `<hostname>/<8 hex chars>` and is **fixed for the process lifetime** — otherwise the release would
  not match its own lock.
- `run` raises no business exception to the caller: every failure converges into the return value
  (the only exception is §7.4).
- Parse / validation errors from `load_spec` are configuration errors and are raised directly (not
  returned as a `RunResult`).

## 3. Result model

### 3.1 Outcome

| Value | Meaning | When produced |
|---|---|---|
| `EXECUTED` | This run acquired the execution right and succeeded | Version not recorded, lock acquired, execution succeeded, success written |
| `SKIPPED_ALREADY_DONE` | This `(task_id, version)` already executed successfully | Step 1 or step 3 hit `status='success'` |
| `SKIPPED_LOCKED` | The execution right was not acquired | The lock is held by another owner whose lease is not expired; or the takeover conditional update affected 0 rows |
| `FAILED` | The execution right was acquired but execution failed (or a precondition of execution was not met) | Handler raised, unexpected HTTP status, missing environment variable, write failure, etc. |

`outcome.to_string()` is always the upper-case constant name from the table above, identical in all
four languages, which keeps metrics and alerting uniform. `EXECUTED` **means success only** — there
is no "executed but failed" case that still reports `EXECUTED`.

### 3.2 RunResult

| Field | Description |
|---|---|
| `task_id` / `version` | Taken from the spec |
| `outcome` | §3.1 |
| `error` | Non-empty only for `FAILED`; holds the original text of the underlying error (no language-specific prefix) |
| `duration_ms` | Wall-clock time from entering `run` until the last release-lock attempt finished |

## 4. Data model and dialects

### 4.1 Tables

```sql
-- Execution lock table: one owner per task_id at a time
CREATE TABLE task_execution_lock (
    task_id      VARCHAR(128) PRIMARY KEY,
    owner_id     VARCHAR(128) NOT NULL,
    acquired_at  TIMESTAMP    NOT NULL,
    expires_at   TIMESTAMP    NOT NULL
);

-- Version record table: (task_id, version) is unique
CREATE TABLE task_version_record (
    task_id      VARCHAR(128) NOT NULL,
    version      VARCHAR(64)  NOT NULL,
    status       VARCHAR(16)  NOT NULL,      -- success | failed
    executed_at  TIMESTAMP    NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    PRIMARY KEY (task_id, version)
);
```

Design notes:

- The lock table uses `task_id` as its primary key, and **the primary key itself is the mutex**:
  among concurrent INSERTs exactly one can succeed and the rest get a conflict error. Hence no
  `SELECT ... FOR UPDATE`, no long transaction, no advisory lock.
- The version table keeps only the latest result per `(task_id, version)` (upsert) and no execution
  history; historical auditing is out of scope for this SDK (the caller records it separately when
  needed).
- The caller creates both tables in the target database; the SDK provides an idempotent
  `ensure_schema` (`CREATE TABLE IF NOT EXISTS`) and performs no schema migration. Full DDL in
  `spec/ddl/{postgres,mysql,sqlite}.sql`.
- Time is always UTC. The SQLite dialect stores it as `YYYY-MM-DD HH:MM:SS.ffffff` text
  (lexicographic order equals chronological order).

### 4.2 SQL

Every statement is **parameterized**, and no SQL contains `now()` / `CURRENT_TIMESTAMP`: the SDK
clock computes the current time and passes it as a bind parameter. That makes the four languages
decide identically and lets tests inject a clock.

```sql
-- Q1 version check (existence means already done)
SELECT 1 FROM task_version_record
 WHERE task_id = ? AND version = ? AND status = 'success'

-- Q2 acquire the lock (primary-key conflict = already taken)
INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at)
VALUES (?, ?, ?, ?)

-- Q3 expired takeover (conditional update, atomic)
UPDATE task_execution_lock
   SET owner_id = ?, acquired_at = ?, expires_at = ?
 WHERE task_id = ? AND expires_at < ?

-- Q4 write the version record (upsert)
-- PostgreSQL / SQLite
INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (task_id, version) DO UPDATE
   SET status = excluded.status,
       executed_at = excluded.executed_at,
       owner_id = excluded.owner_id
-- MySQL / MariaDB
INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    status = VALUES(status), executed_at = VALUES(executed_at), owner_id = VALUES(owner_id)

-- Q5 release (delete only your own lock)
DELETE FROM task_execution_lock WHERE task_id = ? AND owner_id = ?
```

MySQL supplementary rule: `UPDATE` returns the number of *changed* rows by default, so if the
takeover writes values identical to the stored ones (same clock, same owner taking over again) it
may return 0. The MySQL dialect therefore appends one parameterized read-back whenever Q3 returns 0
and only counts the lock as acquired if `owner_id` and `expires_at` both equal the values just
written; PostgreSQL / SQLite decide directly on the affected row count.

### 4.3 Primary-key conflict detection

Acquiring the lock must distinguish "primary-key conflict (the lock is taken)" from "another
database error (a real fault)": the first takes the takeover branch, the second returns `FAILED`
without writing a version record. This is a dialect difference, encapsulated by the dialect layer:

| Dialect | Detection |
|---|---|
| PostgreSQL | SQLState `23505` |
| MySQL / MariaDB | vendor code `1062` (SQLState `23000` is too broad and is never used alone) |
| SQLite | extended code `1555` / `2067` (or SQLState `23505`) |

`INSERT IGNORE` / `INSERT OR IGNORE` are forbidden: they would swallow real faults such as
connection errors or an over-long field as "lock not acquired".

### 4.4 Time binding and transaction boundaries

**Time must be bound as a naive UTC wall clock**, and must not depend on the time zone setting of
the session or the connection pool:

| Dialect | Binding form |
|---|---|
| PostgreSQL | naive `TIMESTAMP`: Python naive `datetime`, Java `Timestamp.from(Instant)` (with an explicitly configured UTC `Calendar`), Go UTC `time.Time`, Rust `NaiveDateTime` |
| MySQL | `DATETIME(6)`: binds the UTC literal text `YYYY-MM-DD HH:MM:SS.ffffff`; Rust can pass `NaiveDateTime` directly |
| SQLite | TEXT, same format as above |

What goes wrong otherwise (all verified): a tz-aware value is shifted by the session time zone (e.g.
`PRC` → 8 hours); the MySQL driver converts times according to the connection `loc` and drops the
fractional seconds of `DATETIME(6)` on MariaDB. Once shifted, the `expires_at < now` decision
depends on each pod's session time zone, which destroys the mutual-exclusion semantics.

**Transaction boundaries**: the SDK does not combine Q1–Q5 into one transaction; one statement is
one unit of work.

- If the connection supplied by the caller has `autoCommit=false`, the SDK commits after each
  successful statement and rolls back after a failed one;
- After a primary-key conflict you must roll back before running Q3, otherwise PostgreSQL reports
  `25P02` because the transaction is in the aborted state;
- Callers **must not** wrap SDK calls in an outer big transaction.

## 5. Coordination algorithm

### 5.1 Sequence

```
Pod A                             DB (lock table / version table)                     Pod B
  │                                           │                                           │
  │ 1. Q1 check version                       │                                           │
  │──────────────────────────────────────────>│                                           │
  │    not found → continue                   │                                           │
  │ 2. Q2 INSERT lock (owner=A)               │                                           │
  │──────────────────────────────────────────>│                                           │
  │    acquired → lock held                   │<─── 2'. Q2 INSERT (owner=B)               │
  │ 3. Q1 double check                        │ conflict → inspect expires_at             │
  │──────────────────────────────────────────>│ not expired → SKIPPED_LOCKED              │
  │    not found → continue                   │ expired → Q3 takeover (rows=1)            │
  │ 4. Execute (http / custom handler)        │                                           │
  │═════════════ executing ═══════════════════│                                           │
  │ 5. Q4 upsert success                      │                                           │
  │──────────────────────────────────────────>│                                           │
  │ 6. Q5 DELETE WHERE owner=A                │                                           │
  │──────────────────────────────────────────>│                                           │
```

### 5.2 Pseudocode

```
run(spec):
  t0 = monotonic_now()
  outcome = null; err = null; lock_held = false
  try:
    # step 1: check the version
    if exists(Q1(task_id, version)):                 # status = 'success'
        outcome = SKIPPED_ALREADY_DONE; return

    # step 2: acquire the execution right (two atomic paths; "check first, then insert" is forbidden)
    now      = clock.now_utc()
    expires  = now + timeout_seconds
    try:
        execute(Q2(task_id, owner_id, now, expires))
        lock_held = true
    except DuplicateKey:
        rows = execute_update(Q3(owner_id, now, expires, task_id, now))
        if rows == 1: lock_held = true
    if not lock_held:
        outcome = SKIPPED_LOCKED; return

    # step 3: double check (during takeover the previous owner may have just written success)
    if exists(Q1(task_id, version)):
        outcome = SKIPPED_ALREADY_DONE; return        # the finally block still releases our own lock

    # step 4 / 4a: execute and record
    execute_executor(spec)
    execute(Q4(task_id, version, 'success', clock.now_utc(), owner_id))
    outcome = EXECUTED
  except anything as e:
    err = message(e)
    outcome = FAILED
    try: execute(Q4(task_id, version, 'failed', clock.now_utc(), owner_id))
    catch: log_warn(...)
  finally:
    if lock_held:
        try: execute(Q5(task_id, owner_id))
        catch e: log_warn(...)                        # a failed release does not change the outcome
  return RunResult(task_id, version, outcome, err, monotonic_now() - t0)
```

### 5.3 Key constraints

1. Step 1 recognizes only `status='success'`; a `failed` record does not prevent a retry.
2. Acquiring the lock has only two paths, Q2 (primary-key conflict) and Q3 (matching conditional
   update), both relying on database atomicity.
3. A failed acquisition returns `SKIPPED_LOCKED`, not `FAILED`.
4. A hit in step 3 returns `SKIPPED_ALREADY_DONE` (not `EXECUTED`, not `SKIPPED_LOCKED`).
5. One acquisition round takes `now` once, so `acquired_at` and `expires_at` are based on the same
   instant.
6. Q5 must carry `owner_id` and must run in `finally` / `defer` / an equivalent epilogue.
7. A failed release is logged only: once the version record is stored the outcome stays `EXECUTED`,
   and the leftover lock is reclaimed by lease expiry.

## 6. Task definition

A task definition is one YAML document with this overall shape (full top-level constraints in §6.4):

```yaml
task_id: config/publish-app-settings    # required, globally unique, ≤128
version: "2026-10-22-v1"                # required, ≤64; a version change under one task_id is a new batch
description: "publish app defaults"     # optional (empty string allowed)
timeout_seconds: 300                    # optional, integer, default 300, must be > 0
execution:
  executor: http                        # http | custom
  config: { ... }                       # fixed shape for http; passed to the handler as-is for custom
```

### 6.1 The http executor

```yaml
execution:
  executor: http
  config:
    method: POST                      # required
    url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"   # required
    headers:                          # optional, values support ${ENV}
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"
      Content-Type: "application/json"
    body_source: file                 # file | inline | none (default none)
    body_path: "app_settings.json"    # required for file; a relative path resolves against the YAML's directory
    body: '{"a":1}'                   # used for inline, supports ${ENV}
    success_check:
      http_status: [200, 201]        # int or list of ints, default 200
```

Success is decided solely by whether the status code falls in `success_check.http_status`. The
failure message format is fixed: `http status 500 not in [200] (body: ...)`, and transport errors
are `http request failed: <reason>`. The response body is used only in the error message, appended
after truncation.

### 6.2 The custom executor

```yaml
execution:
  executor: custom
  handler: "warm_up_cache"            # required, already registered by the caller
  config: { keys: [home:feed], ttl_seconds: 300 }   # optional, passed to the handler as a deep copy; empty map by default
```

A normal return from the handler = success; throwing / returning a non-nil error / returning `Err` =
failure. An unregistered handler → `FAILED` with `error = handler "xxx" is not registered`.

### 6.3 Environment variable substitution

`${NAME}` is replaced with the environment variable in `url`, header values and the inline `body`,
before execution (not at parse time, so one spec can be reused across environments). A missing
variable → `FAILED` with `error = environment variable "NAME" is not set`. `body_path` is never
substituted.

### 6.4 Field validation rules

| Rule | Description |
|---|---|
| Required | `task_id`, `version`, `execution`, `execution.executor` |
| Length | `task_id` ≤ 128, `version` ≤ 64, both non-empty |
| Unknown fields | Rejected (so a typo cannot silently take effect) |
| `timeout_seconds` | Integer, default 300, must be > 0; it is both the lock lease and the HTTP request timeout. When computing the lease it is clamped to a 100-year upper bound (to prevent time-type overflow); validation still only checks `> 0` |
| `executor` | `http` / `custom`, case sensitive, an empty string is invalid |
| `execution.config.method` / `url` | Required when `executor: http` |
| `execution.config.body_path` | Required when `body_source: file`; an empty string counts as missing |
| `execution.handler` | Required when `executor: custom` |
| Empty strings | `description` and an inline `body` may be empty |
| Error messages | Must contain the field path: `task.timeout_seconds must be > 0`, `execution.config.method is required` |

The JSON Schema is in `spec/schema.json`, and the four parsers align with it.

## 7. Error convergence, exceptions and concurrency semantics

### 7.1 Handling per phase

| Phase | Failure handling |
|---|---|
| Step 1 (version check) | Database error → `FAILED`, attempt to write a `failed` record, no lock held |
| Step 2 (acquisition) | Non-conflict exception → `FAILED`, **no** version record, **no** release (the lock is not held yet) |
| Step 3 (double check) | Database error → `FAILED`, write `failed`, release our own lock |
| Step 4 (execution) | Any failure → `FAILED`, write `failed`, release the lock |
| Failure record write | Only a warning is logged if the write fails; the outcome is unchanged |
| Lock release | Only a warning is logged if it fails; the outcome is unchanged |

### 7.2 The boundary of "any exception"

Python / Go / Rust converge any exception in the execution phase (including a Go panic and a Rust
panic) into `FAILED` and guarantee both the `failed` write and the lock release. Java catches
`Throwable`: an ordinary `Error` (e.g. `AssertionError`) also becomes `FAILED`, while
`VirtualMachineError` (OOM / StackOverflow) is logged and rethrown — yet `finally` still releases
the lock. Opening connections and writing to the database on an exhausted heap is unreliable;
letting the container restart is safer. This is the only deliberate difference among the four
languages.

### 7.3 Same-process concurrency and batches

- `run_all` runs serially; results keep the input order and one failure does not affect the rest.
- Calling `run` concurrently in the same process for the same `(task_id, version)`: the first caller
  executes, later ones get `SKIPPED_LOCKED` because the lock has not expired (the owner is the same
  but the INSERT still conflicts). Semantically safe, but a single process has no reason to do this.
- The `Coordinator` handler registry is read-only, so `run` can be called concurrently from multiple
  threads / tasks (each language guarantees this with a concurrent container, a lock or `&self`).
  How the `Store` uses its connection is up to the implementation: Python uses a single connection
  plus a mutex (startup tasks are few, so it suffices), while Go / Java / Rust take a connection pool
  from the caller.

### 7.4 Exceptions that are not converged

Java rethrows `VirtualMachineError`; Python passes `KeyboardInterrupt` / `SystemExit` through. In
every other case `run` returns a `RunResult`.

## 8. Logging and observability

Uniform events: `lock acquired`, `lock takeover`, `skip already done`, `skip locked`, `task failed`,
`release failed`. Log content never takes part in any behavioural decision.

Suggested fields (each language emits them through its own logging facility): `task_id`, `version`,
`owner_id`, `outcome`, `duration_ms` and the original error text. This stays at the logging layer;
built-in metrics / tracing are out of scope for this version (see §11).

## 9. Usage

Setup and handler registration for each language are covered in the per-directory READMEs
(`sdk-python/`, `sdk-go/`, `sdk-java/`, `sdk-rust/`); the common shape is:

```text
store = <PostgresStore | MysqlStore | SqliteStore>(connection info)
ensure_schema(store)
coordinator = Coordinator(store, owner_id)      # or omit owner and use the default <hostname>/<hex>
coordinator.register_handler("warm_up_cache", fn)
results = coordinator.run_all(load_spec("...").for each spec)
```

Example tasks (present under `examples/` in every language, byte-for-byte identical):
- `publish_app_settings.yaml` + `app_settings.json`: the http executor, `PUT` to the config center,
  the header uses `${CONFIG_CENTER_TOKEN}`, the body comes from a file, success codes `[200, 201]`;
- `warm_up_cache.yaml`: the custom executor, handler `warm_up_cache`, config is `keys` + `ttl_seconds`.

**Release discipline**: `version` is the only idempotency key. Changing the YAML content (url, body,
parameters) requires bumping `version` at the same time, otherwise the new content never runs.

## 10. Verification and test matrix

### 10.1 Three test layers

| Layer | Content | Dependency |
|---|---|---|
| Unit | YAML validation, environment variable substitution, status-code decisions, dialect SQL / conflict detection, time formats | no external dependency |
| Scenario simulation | Multiple connections and multiple owners simulate multiple pods, covering the six scenarios of §10.2 | SQLite (file database, WAL) |
| Real database | The same scenarios plus dialect-specific assertions, run against PostgreSQL / MySQL | a real database |

Scenario simulation uses 8 concurrent callers, each holding its own connection, and asserts "exactly
one `EXECUTED`, the rest `SKIPPED_LOCKED`", which stands in for a real multi-replica K8s load test.
A real K8s deployment is out of verification scope.

### 10.2 Mandatory scenarios

| # | Scenario | Assertion |
|---|---|---|
| 1 | First execution on a single pod | `EXECUTED`, a `success` version record, the lock table emptied |
| 2 | Repeat execution | `SKIPPED_ALREADY_DONE`, and the lock table is empty before and after (no lock was taken) |
| 3 | Concurrent acquisition | 8 concurrent → exactly 1 `EXECUTED` and 7 `SKIPPED_LOCKED`, the handler runs once |
| 4 | Expired lock takeover | A pre-seeded expired lock → the new pod reports `EXECUTED` and the lock owner becomes the new pod |
| 5 | Execution failure | `FAILED`, the lock is released, the version record is `failed` (a retry can be `EXECUTED` again) |
| 6 | Release does not delete others' locks | After A's lock is taken over by B, A's release does not affect B's lock |

Additional coverage: non-conflict errors during acquisition (`FAILED` with no record written and no
release), a failed release not changing the outcome, a double-check hit after acquiring the lock,
lease boundaries and durations under an injected clock, and the HTTP executor (a local mock server,
including `${ENV}`, the error text and timeout propagation).

### 10.3 Real-database results

| Dialect | Environment | Coverage |
|---|---|---|
| PostgreSQL | 14.6 (session time zone `PRC`) | DDL, Q1–Q5, 8-way concurrency, takeover, takeover after a conflict on a non-autocommit connection (no `25P02`), time round-trips (including an `Asia/Shanghai` session) |
| MySQL | MariaDB 10.4.24 (session `+08:00`) | DDL, `ON DUPLICATE KEY UPDATE`, `1062` detection, `DATETIME(6)` microseconds, 8-way concurrency, takeover |
| SQLite | local file database | every scenario and unit test |

**Cross-language interop**: on the same PG database and the same MySQL database, after Python
executes and writes success, Go / Java / Rust all return `SKIPPED_ALREADY_DONE`; an expired lock can
be taken over by another language; the time values the four languages write agree to the microsecond
and are independent of the session time zone.

### 10.4 How to run

SQLite tests run out of the box; real-database tests are enabled by
`STARTUP_TASK_PG_DSN` / `STARTUP_TASK_MYSQL_DSN` and skip automatically when absent (with
databases: Python 143 passed / Go 93 PASS / Java 87 passed / Rust 72 passed; without databases:
Python 114 passed + 29 skipped, Go ok, Java 87 run 21 skipped, Rust 72 passed).

## 11. Limitations, trade-offs and risks

1. **Takeover brings duplicate execution.** Once the lock expires a new pod can take over while the
   original pod may still be executing, so two copies run concurrently. This is the inevitable
   trade-off of giving up a separate lock service: converge with `timeout_seconds` (≥ the task's p99
   duration + margin) and idempotent handlers.
2. **The crash window is not absolutely deduplicated.** Being killed between "execution succeeded"
   and "success written" makes the next pod rerun. Without distributed transactions this cannot be
   eliminated.
3. **The lease depends on each pod's clock.** Time is computed by the SDK, so drift between pods
   inflates or shrinks the effective lease; production must guarantee NTP, or set the lease
   conservatively.
4. **A failed release leaves the lock behind.** Once the version record is stored the outcome stays
   `EXECUTED`, and the leftover lock makes other pods wait for expiry.
5. **`timeout_seconds` serves two purposes** (lease + HTTP timeout). When a task's duration
   approaches the timeout you get "still running, but the lock can already be taken over".
6. **The version number is the only idempotency key.** Changing content without bumping the version
   does not rerun; this needs release-process discipline.
7. **MySQL 8 is not verified separately.** Real-database verification used MariaDB 10.4;
   `ON DUPLICATE KEY UPDATE ... VALUES(col)` is marked deprecated as of MySQL 8.0.20+ (it still works
   today, but future versions need an assessment).
8. **HTTP client injectability is uneven.** Java can inject an `HttpClient`; Go / Python / Rust
   currently use built-in clients and need an extension where a custom proxy, mTLS or a self-signed
   CA is required (otherwise only the default proxy environment variables apply).
9. **No built-in metrics / tracing**, logging only; troubleshooting simultaneous multi-replica starts
   depends on log aggregation.
10. **No built-in CI configuration.** Real-database tests are gated by environment variables, so
    wiring them in requires PG / MySQL services in the pipeline; otherwise "only SQLite is running"
    hides dialect, time-zone and precision problems.
11. **Go's `-race` is not part of verification** (the verification environment has no cgo toolchain);
    concurrency correctness is guaranteed by multi-connection real-database tests.
12. **Not implemented**: a separate lock service, other executors, cron scheduling, a UI, an admin
    console.

## 12. Future work

- Open the HTTP executor to client injection (proxy / mTLS / custom CA), aligned across the four
  languages;
- Built-in metrics hooks (executions, skips, failures, acquisition latency) and structured log
  fields;
- Wire CI to real PG / MySQL, and require any dialect change to pass that matrix;
- Verify on MySQL 8 when needed and assess the upsert syntax evolution.
