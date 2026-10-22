Chinese: [README.zh-CN.md](README.zh-CN.md)

# startup-task-sdk (Java)


Java implementation of the cross-language "startup task coordination SDK". When several Kubernetes
pods start at the same time, it guarantees that a given initialization task runs successfully at most
once per `(task_id, version)` (for example, publishing default application settings or warming up a
cache).

The normative behaviour baseline is `../docs/design.md`; this implementation follows it clause by
clause with **no deviations**. All four languages (python / java / go / rust) share the same
coordination algorithm, SQL semantics and outcome enum.

## Requirements and dependencies

- JDK 21 (`java -version` must report 21)
- Maven 3.9+
- The only runtime dependency is `org.yaml:snakeyaml:2.3`; `junit-jupiter` / `sqlite-jdbc` are test
  scope. HTTP uses the JDK's `java.net.http.HttpClient`, and no JSON parsing is required.

## Build and test

```bash
cd sdk-java
mvn -B test
```

> If `mvn` is not installed system-wide, any Maven 3.9+ distribution works (unpack it and call its
> `bin/mvn`); every `mvn` in the commands below refers to that executable.

By default `mvn test` runs real-SQL cases against the SQLite dialect (xerial sqlite-jdbc, test
scope): every thread gets its own connection, each connection sets `PRAGMA busy_timeout=10000`, and
the first connection switches the database to `PRAGMA journal_mode=WAL`. The integration tests
against real PostgreSQL / MariaDB need DSN environment variables and are skipped automatically when
those are absent (see "Integration tests").

## Quick start

```java
import io.startuptask.*;

// 1) Pick a Store (PostgreSQL / MySQL / SQLite)
Store store = new PostgresStore(dataSource);                 // or new PostgresStore(url, user, password)
// Store store = new MysqlStore(dataSource);
// Store store = new SqliteStore("jdbc:sqlite:/tmp/startup-task.db");

// 2) Create the schema (idempotent; safe to call on every start)
Schema.ensure(store);

// 3) Create the coordinator; prefer the pod name as ownerId. Passing null defaults to
//    "<hostname>/<uuid8>", fixed for the lifetime of the process.
Coordinator coordinator = new Coordinator(store, System.getenv("POD_NAME"));

// 4) Register custom handlers (a duplicate name throws IllegalStateException)
coordinator.registerHandler("warm_up_cache", config -> {
    List<String> keys = (List<String>) config.get("keys");
    int ttlSeconds = (int) config.getOrDefault("ttl_seconds", 300);
    // ... business logic
});

// 5) Load the YAML and run it
TaskSpec spec = TaskSpecLoader.load("/etc/startup/publish_app_settings.yaml"); // YAML text works too
RunResult result = coordinator.run(spec);
if (result.failed()) {
    throw new IllegalStateException("startup task failed: " + result.error());
}

// Batch: executed serially, one failure does not stop the rest, results keep the input order
List<RunResult> results = coordinator.runAll(List.of(specA, specB));
```

`RunResult` is immutable, with the fields `taskId / version / outcome / error / durationMs`;
`outcome` is one of `EXECUTED | SKIPPED_ALREADY_DONE | SKIPPED_LOCKED | FAILED`, and `toString()`
looks like `RunResult[taskId=…, version=…, outcome=EXECUTED, error=null, durationMs=35]`.

`run()` throws no checked exceptions: every failure during execution converges to `FAILED` and is
recorded as a version row with `status='failed'`.

## YAML task definition

```yaml
task_id: config/publish-app-settings # required, 1..128, matches [A-Za-z0-9._:/-]+
version: "2026-10-22-v1"             # required, 1..64; a new version starts a new batch per task_id
description: Publish application default settings to the config center  # optional
timeout_seconds: 120                 # optional, default 300, must be > 0; also the zombie-lock
                                     # timeout (lock window capped at 100 years)
execution:                           # required
  executor: http                     # required, http | custom (case sensitive)
```

### executor: http

```yaml
execution:
  executor: http
  config:
    method: PUT                      # required, upper-cased before being handed to HttpClient
    url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"   # required, supports ${ENV}
    headers:                         # optional, string->string, values support ${ENV}
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"
      Content-Type: application/json
    body_source: file                # optional: file | inline | none (default none)
    body_path: app_settings.json     # required when body_source=file
    body: '{"a":1}'                  # used when body_source=inline (optional, empty by default),
                                     # supports ${ENV}
    success_check:
      http_status: [200, 201]        # optional, an int or a list of ints, default 200
```

- `${NAME}` (`[A-Za-z_][A-Za-z0-9_]*`) is substituted **before the HTTP request is sent**, in `url`,
  header values and the inline `body`. File bodies and `body_path` are not substituted.
- A missing variable → `FAILED` with `error = 'environment variable "NAME" is not set'`.
- A status code outside the allow list → `FAILED` with
  `error = 'http status 500 not in [200] (body: …)'`; transport errors →
  `'http request failed: <reason>'`.
- The request timeout is `timeout_seconds` seconds.
- When `body_path` is relative: a spec loaded from a file resolves it against that file's directory;
  a spec loaded from a string resolves it against the process working directory.
- Body files are sent **byte for byte** (`Files.readAllBytes` + `ofByteArray`), so non-UTF-8 payloads
  are not corrupted; a missing relative path → `FAILED` with
  `error = 'body file "…" cannot be read: …'`.
- `description` and the inline `body` accept empty strings; for `body_source: file` an empty
  `body_path` is still rejected as missing.

### executor: custom

```yaml
execution:
  executor: custom
  handler: warm_up_cache                     # required, non-empty, must be registered on the Coordinator
  config: { keys: ["home:feed"], ttl_seconds: 300 }   # optional, deep-copied and passed to the handler
                                                      # verbatim; empty map by default
```

An unregistered handler → `FAILED` with `error = 'handler "xxx" is not registered'`.
A handler that throws → `FAILED` (the `error` field carries the exception message).

### Validation

`TaskSpecLoader.load(...)` throws `SpecException` (a `RuntimeException`) on validation failures; the
message always contains the field path, for example `task.timeout_seconds must be > 0`,
`execution.config.method is required`, `task.task_id must match ^[A-Za-z0-9._:/-]+$`.
Unknown fields are rejected as well (`task.foo is not allowed`).

## Examples

Three files live in `examples/`:

| File | Description |
|---|---|
| `publish_app_settings.yaml` | `executor: http`, `PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings`, header uses `${CONFIG_CENTER_TOKEN}`, body comes from a file |
| `app_settings.json` | The body file of the task above (application default settings) |
| `warm_up_cache.yaml` | `executor: custom`, `handler: warm_up_cache`, `config.keys` + `config.ttl_seconds` |

`io.startuptask.examples.ExampleRunner` is a main class that really runs: it creates the SQLite
database and schema, registers the `warm_up_cache` handler, then calls `runAll` twice (on the second
run successful tasks are `SKIPPED_ALREADY_DONE` and failed tasks are retried, as the contract
requires).

Example environment variables (overridable with a same-named `-D` property or an environment
variable; defaults are used otherwise):

| Variable | Default |
|---|---|
| `CONFIG_CENTER_URL` | `http://127.0.0.1:8080` |
| `CONFIG_CENTER_TOKEN` | `example-token` |
| `APP_NAME` | `demo-app` |

Nothing usually listens on the default address, so the http example ends as `FAILED`
(`http request failed: …` or `http status 404 not in [200, 201] …`) and **never throws**, while the
custom example finishes as `EXECUTED`. To see the success path, point `CONFIG_CENTER_URL` at a real
config center:

```bash
java -DCONFIG_CENTER_URL=http://127.0.0.1:8080 -cp "target/classes;$(cat target/cp.txt)" \
  io.startuptask.examples.ExampleRunner
```

**A command that actually works** (Git Bash, working directory `sdk-java`):

```bash
# Generate the runtime classpath (sqlite-jdbc is test scope, so the example pulls it in with
# includeScope=test)
mvn -q -B test-compile \
  dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=target/cp.txt

java -cp "target/classes;$(cat target/cp.txt)" io.startuptask.examples.ExampleRunner
```

Output looks like this (timestamps, durations and the owner differ on every run):

```
owner=demo-pod/7f6fa50d
sqlite=…/sdk-java/target/examples/startup-task.db
config-center=http://127.0.0.1:8080 app=demo-app
run #1
2026-10-22T09:38:59.409Z INFO lock acquired task=cache/warm-up owner=demo-pod/7f6fa50d
  warm_up_cache: keys=[home:feed, home:recommend, config:feature-flags] ttl_seconds=300
2026-10-22T09:38:59.456Z INFO lock acquired task=config/publish-app-settings owner=demo-pod/7f6fa50d
2026-10-22T09:38:59.567Z WARNING task failed task=config/publish-app-settings owner=demo-pod/7f6fa50d
  RunResult[taskId=cache/warm-up, version=2026-10-22-v1, outcome=EXECUTED, error=null, durationMs=66]
  RunResult[taskId=config/publish-app-settings, version=2026-10-22-v1, outcome=FAILED, error=http status 404 not in [200, 201] (body: 404 page not found), durationMs=182]
run #2 (same specs: success is skipped, failed is retried)
2026-10-22T09:38:59.636Z INFO skip already done task=cache/warm-up version=2026-10-22-v1
2026-10-22T09:38:59.658Z INFO lock acquired task=config/publish-app-settings owner=demo-pod/7f6fa50d
2026-10-22T09:38:59.668Z WARNING task failed task=config/publish-app-settings owner=demo-pod/7f6fa50d
  RunResult[taskId=cache/warm-up, version=2026-10-22-v1, outcome=SKIPPED_ALREADY_DONE, error=null, durationMs=6]
  RunResult[taskId=config/publish-app-settings, version=2026-10-22-v1, outcome=FAILED, error=http status 404 not in [200, 201] (body: 404 page not found), durationMs=56]
```

Delete `target/examples/startup-task.db` (or pass a second argument to point at another state
directory) to replay the first-run behaviour. Without `java -cp` you can also run the examples test
directly:

```bash
mvn -q -B test -Dtest=ExamplesTest
```

## Coordination algorithm

Follows the order of `design.md` §5 exactly:

1. **Check the version**: `SELECT 1 … status='success'` hits → `SKIPPED_ALREADY_DONE` (a `failed`
   row does not block a retry).
2. **Acquire the execution right** (two atomic paths; never `SELECT` followed by `INSERT`):
   - `INSERT INTO task_execution_lock …`; a primary-key conflict means "already held";
   - on a conflict, run
     `UPDATE … SET owner_id=?, acquired_at=?, expires_at=? WHERE task_id=? AND expires_at < ?`;
     only an affected row count of 1 wins the lock (expired-lock takeover).
   Neither path winning → `SKIPPED_LOCKED`.
3. **Double check**: after acquiring the lock, check the version record again; a hit →
   `SKIPPED_ALREADY_DONE` (during a takeover the previous owner may already have written `success`),
   and the lock is still released afterwards.
4. **Execute** the executor (http or custom); on success write `status='success'` → `EXECUTED`, on
   failure write `status='failed'` → `FAILED`.
5. **Release**: `DELETE FROM task_execution_lock WHERE task_id=? AND owner_id=?`, executed in
   `finally` (including the early return from step 3); only the caller's own lock is deleted, and a
   failed release is logged without changing the outcome.

Other conventions:

- Every SQL statement binds parameters through `PreparedStatement` and **never** uses `now()` /
  `CURRENT_TIMESTAMP`: "current time" comes from the `Clock`, and `acquired_at` / `expires_at` share
  the same `now`.
- A **non-conflict** exception during the locking phase (for example a connection error) yields
  `FAILED` immediately, without writing a version record and without releasing the lock.
- The execution phase (steps 4 and 4a) catches `Throwable`: ordinary `Error`s (for example a handler
  throwing `AssertionError`) converge to `FAILED` just like exceptions (write the `failed` record and
  release the lock in `finally`); fatal `VirtualMachineError`s (OOM / StackOverflowError) are logged
  and rethrown without a version record, but `finally` still releases this pod's lock.
- The `expires_at` lock window is capped at 100 years (matching Python/Rust); the `> 0` validation
  rule is unchanged.
- Every store call opens its own connection and does exactly one thing: with an `autoCommit=false`
  connection the SDK commits its successful work and rolls back its failed work itself (on PG a
  primary-key conflict aborts the transaction, so it must be rolled back before Q3 can attempt the
  expired-lock takeover).
- Time is always UTC. SQLite stores the text `YYYY-MM-DD HH:MM:SS.ffffff` (6 fractional digits,
  lexicographic order equals time order); PG binds `Timestamp.from(Instant)` with an explicit UTC
  `Calendar`; MySQL binds UTC text in the same format (`mysql-connector-j` loses fractional seconds
  when calling `setTimestamp` against a MariaDB 10.4 `DATETIME(6)` column — see "Integration tests").
- Primary-key conflict detection is encapsulated in `Dialect`: PG `23505`, MySQL `1062`, SQLite
  `1555`/`2067`/`23505`/messages containing `UNIQUE constraint failed` or `PRIMARY KEY`. `INSERT
  IGNORE` is never used.
- The `Clock` is injectable (`new Coordinator(store, ownerId, clock)`), which keeps tests
  deterministic.

## Logging

Uses `java.util.logging` (no SLF4J dependency). The logs cover `lock acquired` / `lock takeover` /
`skip already done` / `skip locked` / `release failed`. By default the container/parent logger
configuration applies; `Logs.configure(Level.INFO)` installs a compact one-line handler instead.

> Note: xerial sqlite-jdbc resets the JUL configuration when it opens its first connection (handlers
> and levels are cleared). With SQLite, call `Logs.configure(...)` after the first connection is
> opened (`ExampleRunner` does exactly that); PG/MySQL in production are unaffected.

## Integration tests (real PostgreSQL / MySQL)

`IntegrationPgTest` / `IntegrationMysqlTest` run the same scenarios against real databases. The DSN
comes from environment variables and is **only used in process memory**: neither exception messages
nor `toString()` echo the DSN or the password.

| Environment variable | Form | Driver |
|---|---|---|
| `STARTUP_TASK_PG_DSN` | `postgresql://user:password@host:5432/db` | `org.postgresql:postgresql` (42.7.x, test scope) |
| `STARTUP_TASK_MYSQL_DSN` | `mysql://user:password@host:3306/db` | `com.mysql:mysql-connector-j` (9.x, test scope) |

```bash
cd sdk-java

# No DSN: integration tests are skipped, everything else runs as usual
mvn -B -o test

# With DSNs (use your own environment variables, never echo the credentials)
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
mvn -B test
```

Requirements: the databases already exist and the account may create tables and insert/delete rows.
The tests only issue `CREATE TABLE IF NOT EXISTS` (`Schema.ensure`) and rows prefixed with
`it-<TestClass>-<uuid8>-`; they clean up by prefix and **never DROP DATABASE**. When the DSN is
missing, `Assumptions.assumeTrue` skips the whole class, so `mvn test` stays green on machines
without a database.

Every dialect covers:

- `Schema.ensure` really executes: PG asserts `timestamp without time zone` / `character varying(128)`;
  MySQL asserts `datetime` + `datetime_precision=6`.
- The first run is `EXECUTED` (success record, empty lock table); a second run with the same version
  is `SKIPPED_ALREADY_DONE` (the lock table stays empty).
- 8 threads / 8 independent Coordinators / 8 owners → exactly 1 `EXECUTED`, 7 `SKIPPED_LOCKED`, and
  the handler runs once.
- Inserting an already expired lock directly → the new pod is `EXECUTED` and the lock owner is the new
  pod while the handler runs.
- A handler that throws → `FAILED`, the lock is released and a `failed` row is recorded.
- After A acquires the lock it is renamed to B; A's `releaseLock` leaves B's lock in place.

Dialect-specific:

- **PG time zone**: after `TimeZone.setDefault(Asia/Shanghai)`, acquire a lock / write a version
  record with a known UTC instant and read it back with `getTimestamp(col, utcCalendar)`, asserting
  microsecond-exact equality with the UTC instant that was sent.
- **PG non-autocommit (25P02 regression)**: wrap `SimpleDataSource` so it returns `autoCommit=false`
  connections. Pod B first tries an unexpired lock with it (must be `SKIPPED_LOCKED`, not `FAILED`
  from an aborted transaction), then the lock is made expired and Pod B must take it over
  (`EXECUTED`).
- **MySQL**: `ON DUPLICATE KEY UPDATE` keeps a single row per version; `1062` is recognised as "lock
  held" rather than an exception; `DATETIME(6)` still reads back as UTC microsecond-exact under a
  `Asia/Shanghai` default time zone.

## Known trade-offs

- **Takeover can cause duplicate execution**: a lock with `expires_at < now` is taken over by another
  pod while the previous owner may still be running. Per contract §5 point 3 this is an accepted
  trade-off; the SDK gives no absolute de-duplication guarantee for non-idempotent operations under
  crash takeover.
- `timeout_seconds` doubles as the execution timeout and the zombie-lock criterion, so it must exceed
  the task's real worst-case duration or the task will be taken over too early.
- The step 3 double check only narrows the window; it is not a distributed transaction. Concurrent
  writes can still happen between the two checks (harmless, since they target the same
  `(task_id, version)` row).
- A non-conflict exception during the locking phase (connection failure) leaves no version record and
  no released lock, per contract; such cases need manual intervention or a restart.
- When the DataSource returns `autoCommit=false` connections, the SDK commits/rolls back per call as
  one unit of work; if you want to wrap a larger transaction around a connection, do not hand that
  connection to the SDK.
- The integration tests need real database DSNs (skipped when absent). The verification environment
  used PostgreSQL 14.6 and MariaDB 10.4.24: PG's time-zone binding and non-autocommit takeover
  (25P02) were verified; the MySQL dialect binds UTC text (`SqlTypes`) because
  `mysql-connector-j`'s `setTimestamp` loses fractional seconds on MariaDB (reproduced with 8.4.0 /
  9.2.0).

## Layout

```
sdk-java/
├── pom.xml
├── examples/
│   ├── publish_app_settings.yaml
│   ├── app_settings.json
│   └── warm_up_cache.yaml
├── src/main/java/io/startuptask/
│   ├── TaskSpec.java / TaskSpecLoader.java / SpecException.java / Values.java
│   ├── Outcome.java / RunResult.java / TaskHandler.java / TaskExecutionException.java
│   ├── Store.java / Dialect.java / SqlTypes.java / AbstractJdbcStore.java / SimpleDataSource.java
│   ├── PostgresStore.java / MysqlStore.java / SqliteStore.java / Schema.java
│   ├── Coordinator.java / HttpExecutor.java / Clock.java / SystemClock.java / Logs.java
│   └── examples/ExampleRunner.java
└── src/test/java/io/startuptask/
    ├── CoordinatorContractTest.java   # contract §10 six scenarios + double check + step 1 never
    │                                  # touches the lock table
    ├── CoordinatorMiscTest.java       # duplicate registration / unregistered handler / retry /
    │                                  # runAll order / lock-acquire errors
    ├── StoreTransactionTest.java      # autoCommit=false connections: commit / rollback / takeover
    ├── TaskSpecLoaderTest.java        # parsing happy paths and validation failures
    ├── HttpExecutorTest.java          # 200/201/500, ${ENV} substitution, file bodies, transport errors
    ├── ExamplesTest.java              # examples/ parsing and end-to-end execution
    ├── DialectTest.java               # dialect SQL / conflict detection / timestamp format
    ├── IntegrationPgTest.java         # real PG: time zone / non-autocommit 25P02 / shared scenarios
    ├── IntegrationMysqlTest.java      # real MySQL: upsert / 1062 / DATETIME(6) / shared scenarios
    ├── AbstractStoreIntegrationTest.java  # shared integration scenarios and SQL helpers
    ├── JdbcDsn.java                   # DSN parsing (credentials never leak)
    └── SqliteFixture.java / DelegatingStore.java
```
