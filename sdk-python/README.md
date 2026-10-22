Chinese: [README.zh-CN.md](README.zh-CN.md)

# startup-task-sdk (Python)

Python implementation of the cross-language startup task coordination SDK. The
behaviour baseline is [`docs/design.md`](../docs/design.md) at the repository
root; the YAML structure follows [`spec/schema.json`](../spec/schema.json) and
the DDL lives in [`spec/ddl/*.sql`](../spec/ddl).

The same `(task_id, version)` is executed successfully at most once even when
many pods start at the same time: the SDK performs
"check version → acquire the execution right → execute → record the result →
release the lock", while the action itself is declared in YAML (the built-in
`http` executor, or a `custom` handler registered by the caller).

## Installation

```bash
cd sdk-python
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e .          # Windows
# ./.venv/bin/python -m pip install -e .                # Linux / macOS
```

Runtime dependencies: `PyYAML`, `psycopg2-binary`, `requests`; the tests
additionally need `pytest` (`pip install -e . pytest` or
`pip install -r requirements-dev.txt`). The MySQL dialect additionally needs
`PyMySQL` (`pip install -e ".[mysql]"`).

Requires Python >= 3.10.

## Quick start

```python
from startup_task import Coordinator, SqliteStore, ensure_schema, load_spec

store = SqliteStore("startup_tasks.db")   # see "The three stores" below
ensure_schema(store)                      # create the tables (idempotent)

coordinator = Coordinator(store)          # owner_id defaults to <hostname>/<uuid8>
results = coordinator.run_all([
    load_spec("examples/publish_app_settings.yaml"),
    load_spec("examples/warm_up_cache.yaml"),
])
for result in results:
    print(result.task_id, result.version, result.outcome, result.error, result.duration_ms)
```

`load_spec` accepts both YAML text and a file path; a validation failure raises
`SpecError` (a `ValueError` subclass) and the message carries the field path,
for example `task.timeout_seconds must be > 0` or
`execution.config.method is required`.

### The three stores

```python
from startup_task import PostgresStore, MysqlStore, SqliteStore

# SQLite: tests / embedded use; timestamps stored as TEXT 'YYYY-MM-DD HH:MM:SS.ffffff' (UTC)
store = SqliteStore("startup_tasks.db", busy_timeout_ms=10000)

# PostgreSQL: timestamps bound as native TIMESTAMP (UTC); add statement_timeout for hard limits
store = PostgresStore("postgresql://user:pass@host:5432/db?options=-c%20statement_timeout%3D5000")

# MySQL / MariaDB: timestamps bound as native DATETIME(6) (UTC)
store = MysqlStore("mysql://user:pass@host:3306/db?charset=utf8mb4")
```

All three stores implement the same `Store` protocol (the five parameterized
SQL statements Q1–Q5); the dialects differ only in primary-key conflict
detection and timestamp binding. You may also pass a custom `connect` factory to
reuse a connection pool:

```python
store = PostgresStore(connect=pool.getconn)   # the returned psycopg2 connection must allow autocommit
```

In production pass the pod name explicitly as `owner_id`
(`Coordinator(store, owner_id=os.environ["HOSTNAME"])`); the default value is
fixed for the lifetime of the process, which is what lets the release path match
its own lock.

### Registering a custom handler

```python
def warm_up_cache(config: dict) -> None:
    keys = config.get("keys", [])
    ttl_seconds = config.get("ttl_seconds", 600)
    ...  # a normal return = success; raising = FAILED with the message in RunResult.error

coordinator.register_handler("warm_up_cache", warm_up_cache)
```

The handler signature is `Callable[[dict], None]`; the argument is a **deep
copy** of `execution.config` (without the `handler` field), so mutating it does
not affect the spec. Registering the same name twice raises `ValueError`; when a
handler is not registered, `run` returns `FAILED` with
`error = 'handler "xxx" is not registered'`.

## YAML fields

```yaml
task_id: config/publish-app-settings  # required, 1..128, charset [A-Za-z0-9._:/-]
version: "2026-10-22-v1"              # required, 1..64; a new version = a new batch
description: description text         # optional
timeout_seconds: 60                   # optional, integer > 0, default 300; also the zombie-lock window
execution:                            # required
  executor: http                      # http | custom (case sensitive)
```

For `executor: http`:

| Field | Required | Meaning |
|---|---|---|
| `config.method` | yes | HTTP method, upper-cased during parsing |
| `config.url` | yes | Target URL, supports `${ENV}` |
| `config.headers` | no | string → string, values support `${ENV}` |
| `config.body_source` | no | `file` \| `inline` \| `none`, default `none` |
| `config.body_path` | when `file` | Relative path: resolved against the spec file's directory when the spec is loaded from a file, against the process working directory when loaded from a string |
| `config.body` | no | Request body for `inline`, supports `${ENV}`, defaults to the empty string |
| `config.success_check.http_status` | no | Integer or a non-empty list of integers, default `[200]` |

The request timeout is `timeout_seconds` seconds. Failure messages have a fixed
format: `http status 500 not in [200] (body: ...)` / `http request failed: <reason>`.

For `executor: custom`:

| Field | Required | Meaning |
|---|---|---|
| `execution.handler` | yes | Non-empty string, must be a registered handler name |
| `execution.config` | no | Arbitrary map, passed to the handler as-is (deep copy), defaults to `{}` |

Environment variable substitution applies only to `url`, header values and the
inline `body` (never to `body_path`), and happens right before the HTTP request
is issued; a missing variable yields `FAILED` with
`error = 'environment variable "NAME" is not set'`.

## Outcome semantics

| Outcome | Meaning |
|---|---|
| `EXECUTED` | This run acquired the execution right and succeeded; `status='success'` was recorded |
| `SKIPPED_ALREADY_DONE` | A `success` record already exists for `(task_id, version)` (hit in step 1 or in the step-3 double check) |
| `SKIPPED_LOCKED` | The execution right was not acquired (the lock is held by someone else and has not expired) |
| `FAILED` | The execution right was acquired but execution failed, or a precondition of execution failed (missing environment variable, unexpected HTTP status, handler raised) |

`RunResult` fields: `task_id` / `version` / `outcome` / `error` / `duration_ms`;
`str(outcome)` is the upper-case constant name. A `failed` record does not block
a retry — the next pod will execute again.

## Running the examples

```bash
./.venv/Scripts/python.exe examples/run_examples.py
```

`examples/` holds two generic scenarios (identical names and content in all four
languages):

| File | Description |
|---|---|
| `publish_app_settings.yaml` | `executor: http`, `PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings`, body from `app_settings.json` |
| `app_settings.json` | Request body of the task above (feature flags / limits) |
| `warm_up_cache.yaml` | `executor: custom`, `handler: warm_up_cache`, config carries `keys` and `ttl_seconds` |
| `run_examples.py` | Runnable entry point: creates a SQLite database, registers `warm_up_cache`, runs both tasks through `run_all` and prints the `RunResult`s |

`run_examples.py` sets defaults for `CONFIG_CENTER_URL` (default
`http://127.0.0.1:8080`), `CONFIG_CENTER_TOKEN` (default `example-token`) and
`APP_NAME` (default `demo-app`); running `run_all` a second time shows
`SKIPPED_ALREADY_DONE` (the third execution, with a bumped version, is
`EXECUTED` again). Without a usable config center (connection failure or a
non-2xx response) the http task reports `FAILED` and prints the reason — the
script does not crash and exits with status 0.

## Running the tests

```bash
./.venv/Scripts/python.exe -m pytest -q
```

The tests use a real SQLite file database (WAL + `busy_timeout=10000` +
independent connections) and never mock SQL; they cover the 6 scenarios of
`docs/design.md` §10, spec validation negatives, the http executor (against a
local HTTP server) and the Outcome string contract. Real-database integration
tests are described in the next section and skip automatically when no DSN is
configured.

## Integration tests (real PostgreSQL / MySQL)

`tests/test_integration_common.py`, `tests/test_integration_pg.py` and
`tests/test_integration_mysql.py` connect to real databases and read their
connection strings from the `STARTUP_TASK_PG_DSN` / `STARTUP_TASK_MYSQL_DSN`
environment variables (**read from the environment only — never written to disk,
never printed**). Cases whose variable is unset are skipped, so `pytest -q`
stays green without a database.

```bash
# provide the connection strings yourself (never commit them)
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
./.venv/Scripts/python.exe -m pytest -q

# run only the integration tests
./.venv/Scripts/python.exe -m pytest -q tests/test_integration_common.py \
    tests/test_integration_pg.py tests/test_integration_mysql.py
```

```powershell
# PowerShell
$env:STARTUP_TASK_PG_DSN = "postgresql://user:password@host:5432/startup_task_sdk_selftest"
$env:STARTUP_TASK_MYSQL_DSN = "mysql://user:password@host:3306/startup_task_sdk_selftest"
```

DSN formats: `postgresql://user:password@host:port/db` (handed to
psycopg2/libpq) and `mysql://user:password@host:port/db?charset=utf8mb4`
(decomposed into PyMySQL keyword arguments by `parse_mysql_dsn`). Only an empty
database has to exist beforehand — `ensure_schema(store)` creates the tables.
The tests **never run `DROP DATABASE`**; every case uses a unique
`it/<dialect>-<case>-<random>` task_id and deletes exactly the rows it wrote.

Coverage:

- Shared by both dialects (`test_integration_common.py`, parameterized over
  pg / mysql): `ensure_schema` creates the tables on a real server and the
  column types are checked (PG `VARCHAR/TIMESTAMP`,
  MySQL/MariaDB `VARCHAR(128)`/`DATETIME(6)`); the first run yields `EXECUTED`
  plus a `success` record and empties the lock table; re-running the same
  version yields `SKIPPED_ALREADY_DONE` with the lock table empty before and
  after; 8 independent connections / 8 owners in parallel produce exactly one
  `EXECUTED`, 7 `SKIPPED_LOCKED` and a single handler call; an expired lock is
  taken over (the lock owner is already the new pod while it executes); a
  handler that raises yields `FAILED`, the lock is released and a `failed`
  record is written; A's release path does not delete a lock that was renamed to
  B; a version bump counts as a new batch.
- PostgreSQL only (`test_integration_pg.py`): with a non-autocommit connection
  (`autocommit=False` injected through `connect=`), both outcomes of the Q3
  takeover after a Q2 primary-key conflict (unexpired → `SKIPPED_LOCKED`,
  expired → `EXECUTED`), asserting that `25P02` never appears and that the
  result is never misreported as `FAILED`; timestamp binding asserts that
  `acquired_at/expires_at` match the dispatched UTC values exactly, and the
  session time zone is additionally set to `America/New_York` (the default
  session is `PRC`) to prove independence from the session tz and that expiry is
  still judged correctly across different session time zones.
- MySQL / MariaDB only (`test_integration_mysql.py`): `ON DUPLICATE KEY UPDATE`
  really upserts (same version, first `failed` then `success`, leaving one row
  with `success`); a real `1062` is classified as a lock conflict
  (`DuplicateKeyError` / `SKIPPED_LOCKED`, not `FAILED`); `DATETIME(6)` reads
  back as `datetime`, keeps microseconds and holds the UTC wall clock.

## Known trade-off: takeover can execute twice

When a pod holding the lock crashes, the lock is not deleted automatically;
after it expires (`expires_at < now`) a new pod takes it over with a conditional
`UPDATE` and runs the task. The **original owner may still be executing**, so
the same `(task_id, version)` can run twice — a trade-off the contract accepts
explicitly (§5 point 3, §11). The SDK does not provide an absolute
deduplication guarantee for non-idempotent operations under crash takeover.

Engineering mitigations:

- Make the task itself idempotent (`PUT` instead of `POST`, or read-then-write).
- Set `timeout_seconds` to an upper bound of the task's maximum duration; too
  short raises the chance of a false takeover, too long slows down recovery.
- Set a database-side statement timeout / connection timeout so a long hang is
  not mistaken for a zombie owner.
- Do not use the lock table as business state; keep business state outside
  `task_version_record`.

Other implementation notes:

- All timestamps are computed by the SDK clock and passed as bind parameters;
  no `now()` / `CURRENT_TIMESTAMP` appears in SQL, which is what makes injected
  `clock` values useful for deterministic tests.
- The DDL is embedded in `startup_task/schema.py` (statement-for-statement
  identical to `spec/ddl/*.sql`, verified by a test) so an installed wheel does
  not depend on the repository layout.
- There are only two atomic ways to acquire the lock: the Q2 `INSERT` with
  dialect-specific primary-key conflict detection, or the Q3 conditional
  `UPDATE` takeover. There is no "SELECT first, then INSERT".
- MySQL reports *changed* rows rather than *matched* rows, so re-taking a lock
  with the same clock tick and the same owner makes Q3 report 0 rows; the SDK
  then reads the lock row back (`owner_id` and `expires_at` must both equal the
  values just written to count as acquired), while PG/SQLite still decide on
  `rowcount == 1`.
- Every SQL statement is its own unit of work: if the caller injects a
  connection with `autocommit=False` (the psycopg2 default), the SDK commits
  after a successful statement and rolls back after a failed one, so PostgreSQL
  cannot be left in the aborted state (25P02) by a Q2 primary-key conflict and
  break the following Q3, and no unfinished transaction is left behind.
- When computing `expires_at`, `timeout_seconds` is clamped to a 100-year upper
  bound (aligned with the Rust implementation) so extreme values cannot make
  `timedelta` overflow; the validation rule is still `> 0`.
- `description` and an inline `body` may be empty strings; with
  `body_source: file` an empty `body_path` counts as missing and is rejected.
- Timestamps are always bound as the **UTC wall clock**: SQLite uses
  `TEXT 'YYYY-MM-DD HH:MM:SS.ffffff'`, PostgreSQL / MySQL receive a **naive**
  `datetime`. PG columns are `TIMESTAMP` (without time zone) and psycopg2
  renders a tz-aware value in the **session** time zone (e.g. with a `PRC`
  session, UTC 12:00 would be written as 20:00), so naive UTC is mandatory; that
  way pods with different session time zones still compare the same timestamps
  correctly.
- The `mysql://` DSN of `MysqlStore` is decomposed into PyMySQL keyword
  arguments by `parse_mysql_dsn()` (PyMySQL has no libpq-style DSN), and query
  parameters such as `charset` are passed through untouched.
