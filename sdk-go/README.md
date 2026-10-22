Chinese: [README.zh-CN.md](README.zh-CN.md)

# startup-task-sdk / Go


The Go implementation of the cross-language startup task coordination SDK. The
normative behaviour lives in [`../docs/design.md`](../docs/design.md), and this
package follows it clause by clause: when several Pods/processes start at the same
time, the same `(task_id, version)` produces exactly one `EXECUTED`, and every
other instance ends up either `SKIPPED_ALREADY_DONE` or `SKIPPED_LOCKED`.

```go
import startuptask "github.com/kuun993/startup-task-sdk/go"
```

## 1. Initialization

The three Store constructors map to the three SQL dialects. The `*sql.DB` is
supplied by the caller (the caller also imports the driver):

```go
// SQLite (built-in / tests / single node)
db, _ := sql.Open("sqlite", "file:/tmp/startup.db?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)")
db.SetMaxOpenConns(4)
store := startuptask.NewSQLite(db)

// PostgreSQL
store = startuptask.NewPostgres(db)

// MySQL
store = startuptask.NewMySQL(db)

if err := startuptask.EnsureSchema(ctx, store); err != nil {
    log.Fatal(err)
}

// owner_id left empty => "<hostname>/<random8>", computed once per process
// (sync.Once) so that releasing the lock always matches ourselves
coordinator := startuptask.NewCoordinator(store, os.Getenv("POD_NAME"))

// When you need a controllable clock (tests / replay):
coordinator = startuptask.NewCoordinatorWithClock(store, podName, myClock) // myClock implements Clock
```

When SQLite is used for concurrent scenarios it must run with `journal_mode=WAL`
plus `busy_timeout >= 5000`; otherwise you will see `database is locked` instead of
the contract's `SKIPPED_LOCKED`.

For PostgreSQL watch the `sslmode` in the DSN: lib/pq defaults to `require`, so a
server without TLS reports `pq: SSL is not enabled on the server`. Add
`?sslmode=disable` explicitly in that case (or configure certificates as your
environment requires). The MySQL DSN needs `parseTime=true`, otherwise
`DATETIME(6)` cannot be scanned into `time.Time`.

## 2. Registering a custom handler

```go
err := coordinator.RegisterHandler("warm_up_cache", func(config map[string]any) error {
    keys, _ := config["keys"].([]any)            // YAML lists decode to []any
    ttlSeconds, _ := config["ttl_seconds"].(int) // YAML integers decode to int
    return warmUpCache(keys, ttlSeconds)
})
```

- Handler signature is `func(config map[string]any) error`: returning `nil` means
  success, returning an error means failure, and a panic is recovered and turned
  into `FAILED` as well.
- The `config` handed to the handler is a deep copy of `execution.config` without
  the `handler` field; it defaults to an empty map.
- Registering the same name twice returns an error.

## 3. Running

```go
spec, err := startuptask.LoadSpec("/etc/tasks/publish_app_settings.yaml") // file path or YAML text

result, err := coordinator.Run(ctx, spec) // err is always nil; failures show up in result.Outcome / result.Err
log.Printf("%s %s %s %.1fms", result.TaskID, result.Version, result.Outcome, result.DurationMs())

// Serial batch run: one failure does not affect the rest, results keep the input order
results := coordinator.RunAll(ctx, []startuptask.TaskSpec{specA, specB})
```

`Run` never panics and never throws failures back at the caller: every execution
failure is folded into `Outcome == FAILED` plus `Err`.

| Outcome | Meaning |
|---|---|
| `EXECUTED` | This run won the execution right and succeeded (a `status='success'` record was written) |
| `SKIPPED_ALREADY_DONE` | A success record already exists for `(task_id, version)` |
| `SKIPPED_LOCKED` | This run did not obtain the execution right |
| `FAILED` | The execution right was obtained but execution (or a precondition) failed |

`RunResult` fields: `TaskID`, `Version`, `Outcome`, `Err`, `Duration`, plus
`DurationMs()` returning a float in milliseconds (measured from entering `Run`
until the last release-lock attempt finished).

### Clock injection

The contract (§4.2) requires "current time" to come from the SDK clock and to be
passed as a bound parameter, so the clock is replaceable:

```go
type Clock interface{ Now() time.Time }

startuptask.NewCoordinator(store, owner)                   // SystemClock (default)
startuptask.NewCoordinatorWithClock(store, owner, myClock) // injected clock
```

With an injected clock, `acquired_at` / `expires_at` / `executed_at` and
`RunResult.Duration` are all derived from it, so tests can assert exact values (see
`TestInjectedClockDrivesTimestampsAndDuration`, `TestInjectedClockExpiryBoundary`).
A clock implementation must be thread-safe (`Run` may be called concurrently from
several goroutines).

## 4. YAML fields

Top level:

| Field | Required | Description |
|---|---|---|
| `task_id` | yes | 1..128, matches `^[A-Za-z0-9._:/-]+$` |
| `version` | yes | 1..64; a new version under the same task means a new batch |
| `description` | no | Free-form text, empty string allowed |
| `timeout_seconds` | no | Default 300, must be > 0 and must be an **integer** (`300.0` is rejected); used as the HTTP request timeout and as the lock lease, clamped to 100 years |
| `execution` | yes | See below |

`execution`:

| Field | Description |
|---|---|
| `executor` | `http` or `custom` (case sensitive) |
| `handler` | Required when `executor=custom`; name of a handler registered by the caller |
| `config` | Fixed structure for `http` (see below); deep-copied to the handler as-is for `custom` |

`execution.config` (with `executor=http`):

| Field | Description |
|---|---|
| `method` | Required; upper-cased before it reaches the HTTP client |
| `url` | Required; supports `${ENV}` |
| `headers` | Optional, string→string; values support `${ENV}` |
| `body_source` | Optional: `file` \| `inline` \| `none` (default `none`) |
| `body_path` | Required and non-empty when `body_source=file`; a relative path resolves against the spec source (loaded from a file → that file's directory, loaded from a string → process working directory). No environment substitution |
| `body` | Request body when `body_source=inline`; supports `${ENV}`, defaults to the empty string (an empty string is valid and sends an empty body) |
| `success_check.http_status` | int or list of ints, default `200` (integers only as well, `200.0` is rejected) |

Empty-string rules match the other languages: `description`, inline `body` (and
`headers: {}`) may be empty; required fields (`task_id`, `version`, `method`,
`url`, `handler`, `body_path`) report `is required` when empty.

Environment substitution happens **before the request is sent** (`url`, header
values, inline body); `NAME` matches `[A-Za-z_][A-Za-z0-9_]*`. A missing variable
yields `FAILED` with the fixed message `environment variable "NAME" is not set`.

Failure messages are fixed: `http status 500 not in [200] (body: ...)` and
`http request failed: <reason>`.

When validation fails, the error returned by `LoadSpec` always carries the field
path, for example `task.timeout_seconds must be > 0`,
`execution.config.method is required`,
`execution.config.body_path is required when body_source is file`.

## 5. Running the tests

```bash
cd sdk-go
go mod tidy
go test ./... -count=1
go test ./... -race -count=1   # needs CGO_ENABLED=1 and a C compiler in the environment
go run ./examples
```

The verification environment (Windows, no gcc) does not enable `-race`, so
`go test ./... -count=1` is the entry point; the concurrency scenario can be
repeated with `go test -run TestConcurrentOnlyOneExecutes -count=5`.

All tests run on the SQLite dialect with real SQL (not mocks) and cover the six
scenarios of contract §10:

| Test | Scenario |
|---|---|
| `TestFirstRunExecuted` | 1. Empty database → `EXECUTED`, success record, lock released |
| `TestSecondRunSkippedAlreadyDone` | 2. Second run → `SKIPPED_ALREADY_DONE`; the lock table is empty before and after |
| `TestConcurrentOnlyOneExecutes` | 3. 8 goroutines (each with its own `*sql.DB`) → 1 `EXECUTED` + 7 `SKIPPED_LOCKED`, handler called exactly once |
| `TestExpiredLockTakeover` | 4. Expired lock → the new Pod reports `EXECUTED` and owns the lock row while running |
| `TestFailureReleasesLockAndNoSuccess` | 5. Handler error → `FAILED`, lock released, record is `failed`; retrying the same version → `EXECUTED` with success overwriting failed (still a single row) |
| `TestReleaseDoesNotDeleteOtherOwnerLock` | 6. A's release path does not delete the lock owned by B (calls `releaseLock` directly; a full-`Run` variant exists too) |

Also covered: `TestDoubleCheckSkipsWhenSuccessAppeared` (double check after
acquiring), `TestLockedByOtherOwnerSkips`, `TestHandlerPanicBecomesFailed`,
`TestHandlerReceivesCopiedConfig`, the HTTP executor (success / unexpected status /
transport error / missing environment variable), spec validation negatives
(including `timeout_seconds: 300.0` and `body_path: ""`, i.e. float and empty-string
rules), dialect conflict detection and time binding, plus:

| Test | Coverage |
|---|---|
| `TestAcquireErrorFailsWithoutRecordsOrRelease` | Non-conflict SQL error during acquisition → `FAILED`, no version record, no release |
| `TestReleaseFailureDoesNotChangeOutcome` | Q5 failure is logged only → outcome stays `EXECUTED`, the lock survives until its lease ends |
| `TestInjectedClockDrivesTimestampsAndDuration` | The injected clock drives `acquired_at` / `expires_at` / `executed_at` / `Duration` |
| `TestInjectedClockExpiryBoundary` | `expires_at == now` still counts as held, `now - 1µs` can be taken over (strict `<`) |
| `TestHugeTimeoutIsClampedWhenComputingExpiresAt` | Extreme `timeout_seconds` → `expires_at` clamped to 100 years, no overflow |
| `TestDefaultOwnerIDIsProcessStable` | The default owner is computed once per process; an explicit owner is used verbatim |

### Integration tests (real PostgreSQL / MySQL)

`integration_test.go` runs the same coordination scenarios against real databases.
**It skips automatically when the DSN is absent**, so `go test ./...` stays green on
machines without databases.

```bash
# Keep the DSNs in environment variables only; never put them in commands or logs
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'

go test ./... -count=1                      # both dialects
go test . -run TestIntegrationPostgres -v    # PostgreSQL only
go test . -run 'TestIntegrationMySQL/upsert' -v
```

| Environment variable | Purpose |
|---|---|
| `STARTUP_TASK_PG_DSN` | `postgresql://user:pw@host:port/db` (the test appends `sslmode=disable` when the DSN does not specify one) |
| `STARTUP_TASK_MYSQL_DSN` | `mysql://user:pw@host:port/db` (converted to a go-sql-driver DSN and forced to `parseTime=true&loc=UTC`) |

Test constraints: every case creates its own rows under a unique `task_id` prefix
(`itsdk-<dialect>-<nanoseconds>`) and deletes them in `t.Cleanup` by prefix; tables
are created with `EnsureSchema` only and databases/tables are **never dropped**;
credentials enter the process through environment variables only and are never
printed.

Common sub-cases: `ensure_schema`, `first_run_executed`,
`second_run_skipped_already_done`, `concurrent_only_one_executes` (8 independent
`*sql.DB`), `expired_lock_takeover`, `failure_releases_lock_and_no_success`,
`release_does_not_delete_other_owner_lock`, plus dialect-specific ones:

| Dialect | Dedicated cases |
|---|---|
| PostgreSQL | `utc_time_binding` (reads `acquired_at`/`expires_at` back as the same UTC instants with zero offset); `timezone_session_keeps_lease_semantics` (writes, reads and the strict `<` boundary stay correct under `SET TIME ZONE 'Asia/Shanghai'`) |
| MySQL | `duplicate_key_1062` (vendor code 1062 classified as "lock held", not a failure); `upsert_replaces_failed_with_success` (failed then success for the same version leaves one row holding success); `datetime6_round_trips_as_utc`; `stored_wall_clock_is_utc` (writer DSN uses `loc=Asia/Shanghai`, a UTC reader still sees the same instant and the stored literal is UTC); `lease_boundary` (`expires_at == now` still held, `now-1µs` taken over) |

MySQL DSN advice: `parseTime=true` is mandatory, otherwise `DATETIME(6)` cannot be
scanned into `time.Time`. `loc` does not affect what the SDK writes (see below), but
it does affect the time zone of the `time.Time` values you read yourself, so
`loc=UTC` is recommended.

## 6. Implementation notes

Coordination algorithm (`coordinator.go`, strictly following contract §5):

1. Q1 looks for a `status='success'` version record; a hit returns `SKIPPED_ALREADY_DONE`.
2. Read `now` once (UTC); `expires = now + timeout_seconds` (`timeout_seconds` above
   100 years is treated as 100 years to avoid `time.Duration` overflow).
3. Q2 atomically `INSERT`s the lock; on a primary-key conflict, Q3 conditionally
   `UPDATE`s (`expires_at < now`) to take over an expired lock. Neither step does
   "SELECT first, then INSERT".
4. After winning the lock, Q1 runs again (during takeover the previous owner may
   already have written success). A hit returns `SKIPPED_ALREADY_DONE`, and the lock
   is released by `defer`.
5. The executor runs (`http` / `custom`); on success Q4 writes `success` and the run
   returns `EXECUTED`; on failure it writes `failed` and returns `FAILED`.
6. `defer` executes Q5 `DELETE ... WHERE task_id = ? AND owner_id = ?`; a failure is
   logged only and never changes the outcome.

Other constraints:

- All SQL is parameterized and **never** contains `now()` / `CURRENT_TIMESTAMP`;
  the SDK clock always supplies a UTC value that is then bound as a parameter.
  PostgreSQL binds `time.Time` (UTC), MySQL binds a UTC literal
  `YYYY-MM-DD HH:MM:SS.ffffff`, SQLite binds TEXT in the same format
  (lexicographic order equals chronological order).
- Why MySQL binds a literal instead of `time.Time`: go-sql-driver converts a
  `time.Time` into the DSN's `loc` zone before writing, so with `loc=Local` the
  database would hold local wall clock, which conflicts with the "DATETIME(6) holds
  UTC" contract and puts a Go Pod a whole zone offset away from its Python/Java/Rust
  peers. Literal binding makes both the write and the `expires_at < ?` comparison
  independent of any session/DSN time zone.
- Q4 upsert differs per dialect: PostgreSQL/SQLite use
  `ON CONFLICT (task_id, version) DO UPDATE`, MySQL uses
  `ON DUPLICATE KEY UPDATE ... VALUES(...)`.
- Duplicate-key detection is encapsulated in the dialect layer: PostgreSQL SQLState
  `23505`; MySQL vendor code `1062` (also accepting SQLState `23000` plus
  `Duplicate entry`); SQLite extended code `1555`/`2067`, SQLState `23505`, or an
  error message containing `UNIQUE constraint failed` /
  `PRIMARY KEY constraint failed`. The MySQL driver is not a dependency, so the
  vendor code is read reflectively from the driver error's `Number` field without
  adding one.
- A non-conflict SQL error during acquisition (Q2/Q3) is handled per contract §7:
  `FAILED`, with **no** version record and no release, because the lock ownership
  cannot be determined safely at that point.
- Logging uses `log/slog` and covers `lock acquired` / `lock takeover` /
  `skip already done` / `skip locked` / `release failed`;
  `Coordinator.SetLogger` lets you replace the logger.
- The default `owner_id` is cached by a package-level `sync.Once`
  (`<hostname>/<8 lowercase hex chars>`) and stays stable for the process lifetime;
  an explicitly passed `ownerID` is used verbatim.
- The `Clock` interface (`SystemClock` by default) is the single time source:
  `acquired_at`, `expires_at`, `executed_at` and `RunResult.Duration` are all
  derived from it, which is why `Duration` is exactly assertable with an injected
  clock.
- Pluggable storage: `Store` is an interface; implement its five methods when you
  need a different backend.

## 7. Known trade-offs

- **Takeover can cause duplicate execution**: when `expires_at < now` a new owner
  may take over while the previous owner is still running, so a non-idempotent
  operation can still run twice with a small probability. This is the trade-off
  accepted explicitly by contract §5: the SDK only guarantees "at most one
  `success` record per `(task_id, version)`", not physical single execution.
- **The lease is `timeout_seconds`**: when execution approaches or exceeds
  `timeout_seconds`, other Pods take over and execute again. Set `timeout_seconds`
  to an upper bound of the execution time (the HTTP executor also uses it as the
  request timeout).
- **Release failures are only logged**: when Q5 fails (e.g. a network hiccup) the
  outcome is unchanged and the lock survives until `expires_at`, after which it can
  be taken over.
- **A `failed` record does not block retries**: only `success` skips, so the next
  startup runs the task again after a failure.
- **With a fake clock, `Duration` is the fake clock's delta too**: `Duration` is
  defined as "entering `Run` → the last release-lock attempt finished", and both
  ends read the `Clock`. With `SystemClock` that is real elapsed time; with an
  injected test clock it is that clock's delta (which is exactly why injecting one
  makes the value deterministic).
- **Wall-clock granularity bounds `Duration` precision**: on some platforms the
  clock quantum is coarse (about 1ms measured on Windows), so sub-millisecond tasks
  may measure `0ms`. Treat `Duration` as an order of magnitude, not a benchmark, and
  inject a `Clock` when you need an exact value.
- **SQLite is for tests/embedded use only**: concurrent writes are serialized at the
  library level; use PostgreSQL / MySQL for cross-Pod coordination.

## 8. Examples

`examples/` contains two ready-to-run tasks (identical names and content in all four
languages):

```bash
cd sdk-go
go run ./examples
```

| File | Description |
|---|---|
| `examples/publish_app_settings.yaml` | `executor: http`: `PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings` with `Authorization: Bearer ${CONFIG_CENTER_TOKEN}`, `body_source: file` reading `app_settings.json`, `success_check.http_status: [200, 201]` |
| `examples/app_settings.json` | Request body of the task above (feature flags + limits) |
| `examples/warm_up_cache.yaml` | `executor: custom` + `handler: warm_up_cache`, with `keys` and `ttl_seconds` in `config` |
| `examples/main.go` | Creates the SQLite database, registers the `warm_up_cache` handler, `RunAll`s both tasks and prints the results |

When the environment variables are unset, `go run ./examples` applies the example
defaults `CONFIG_CENTER_URL=http://127.0.0.1:8080`,
`CONFIG_CENTER_TOKEN=example-token` and `APP_NAME=demo-app`. If the config center is
unreachable that task reports `FAILED` and prints the reason (no panic), while the
custom task reports `EXECUTED`.

The examples use `%TEMP%/startup-task-sdk-examples.db` by default (override with
`TASK_DB_PATH`). On the second run the custom task becomes
`SKIPPED_ALREADY_DONE` (the http task retries, because a `failed` record does not
block a rerun); delete that database file to start over.
