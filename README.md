Chinese: [README.zh-CN.md](README.zh-CN.md)

# Startup task coordination SDK (Java / Python / Go / Rust)

When several Kubernetes replicas start at once, their initialization actions (publishing default
configuration to the config center, warming hot caches, syncing user permissions, ...) **must not
run concurrently**, and a given version should **succeed exactly once**. This repository ships one
SDK per language, all four isomorphic: the coordination logic "check version → acquire the
execution right → execute → record the result → release the lock" is collapsed into a single
implementation, and what actually runs is declared by the caller in YAML.

- Coordination state lives in the **application's own relational database** (PostgreSQL / MySQL,
  SQLite for tests and embedded use). No Redis, no separate lock service.
- All four languages share one design baseline, [`docs/design.md`](docs/design.md): algorithm steps,
  SQL, result enum and error-convergence rules match item by item.
- Duplicate execution caused by expiry takeover is a **trade-off accepted by design**, see
  "Known trade-offs".

## Directory layout

```
startup-task-sdk/
├── README.md                   ← this file (English overview)
├── README.zh-CN.md             ← Chinese overview
├── LICENSE                     ← MIT
├── docs/
│   ├── design.md               ← overall design (algorithm, data model, conventions, test matrix)
│   └── design.zh-CN.md         ← overall design (Chinese)
├── spec/
│   ├── schema.json             ← JSON Schema of the task definition
│   └── ddl/{postgres,mysql,sqlite}.sql
├── sdk-python/                 ← pip package startup_task
├── sdk-go/                     ← package startuptask
├── sdk-java/                   ← Maven project io.github.kuun993:startup-task-sdk
└── sdk-rust/                   ← crate startup-task-sdk
```

Each language directory has its own README covering setup, handler registration, the YAML
reference, test commands and implementation details.

## Core concepts

| Concept | Description |
|---|---|
| Task | One initialization action to run at startup; `task_id` is globally unique |
| Version | Execution batch; if `(task_id, version, success)` exists in the database, skip |
| Owner | The executor that won the lock (pod name or UUID, fixed for the process lifetime) |
| Executor | `http` (built into the SDK) or `custom` (a handler registered by the caller) |
| Outcome | `EXECUTED` / `SKIPPED_ALREADY_DONE` / `SKIPPED_LOCKED` / `FAILED` |

`RunResult` carries the same fields in all four languages: `task_id`, `version`, `outcome`, `error`,
`duration_ms`. The string form of `outcome` is always the upper-case constant name, which keeps
metrics and alerting uniform.

## Coordination flow

```
Pod A                         DB (lock table / version table)                       Pod B
  │ 1. Check version (success?)             │                                           │
  │────────────────────────────────────────>│                                           │
  │    not found → continue                 │                                           │
  │ 2. INSERT lock (owner=A, now, lease)    │                                           │
  │────────────────────────────────────────>│<─── 2'. INSERT lock (owner=B) → conflict  │
  │    acquired → lock held                 │ not expired → SKIPPED_LOCKED              │
  │ 3. Check version again (double check)   │ expired → UPDATE takeover                 │
  │────────────────────────────────────────>│                                           │
  │ 4. Execute (http / custom handler)      │                                           │
  │ 5. UPSERT version record = success      │                                           │
  │────────────────────────────────────────>│                                           │
  │ 6. DELETE lock WHERE owner=A (finally)  │                                           │
  │────────────────────────────────────────>│                                           │
```

Three rules that hold no matter what:

1. Acquiring the lock uses only two atomic operations: an `INSERT` that hits a primary-key conflict,
   and `UPDATE ... WHERE expires_at < ?` affecting one row. **Checking first and then inserting is
   forbidden.**
2. Releasing the lock must carry `owner_id` and must happen on every exit path (success, failure,
   panic, early return).
3. No database time function appears in any SQL statement: the SDK clock computes the current time
   and passes it as a parameter, so all four languages decide identically and tests can inject a
   clock.

## Quick start

API name mapping:

| Capability | Python | Java | Go | Rust |
|---|---|---|---|---|
| Construct | `Coordinator(store, owner_id=...)` | `new Coordinator(store, ownerId)` | `NewCoordinator(store, ownerID)` | `Coordinator::new(Arc<dyn Store>, owner)` |
| Register a handler | `register_handler(name, fn)` | `registerHandler(name, lambda)` | `RegisterHandler(name, fn)` | `register_handler(name, Box::new(f))` |
| Load a definition | `load_spec(text_or_path)` | `TaskSpecLoader.load(...)` | `LoadSpec(...)` | `load_spec(path)` |
| Single task | `run(spec)` | `run(spec)` | `Run(ctx, spec)` | `run(&spec).await` |
| Batch | `run_all(specs)` | `runAll(specs)` | `RunAll(ctx, specs)` | `run_all(&specs).await` |
| Create tables | `ensure_schema(store)` | `Schema.ensure(store)` | `EnsureSchema(ctx, store)` | `ensure_schema(&store).await` |

**Python**

```python
from startup_task import Coordinator, PostgresStore, ensure_schema, load_spec

store = PostgresStore("postgresql://app:secret@db:5432/app")
ensure_schema(store)

coordinator = Coordinator(store)                  # owner_id defaults to <hostname>/<uuid8>
coordinator.register_handler("warm_up_cache", lambda config: do_warm_up(config))
coordinator.run(load_spec("/etc/startup/publish_app_settings.yaml"))
```

**Java**

```java
PostgresStore store = new PostgresStore(url, user, password);
Schema.ensure(store);

Coordinator coordinator = new Coordinator(store, null);
coordinator.registerHandler("warm_up_cache", config -> doWarmUp(config));
coordinator.run(TaskSpecLoader.load(Path.of("/etc/startup/publish_app_settings.yaml")));
```

**Go**

```go
store := startuptask.NewPostgres(db)              // db comes from the caller: sql.Open("postgres", dsn)
_ = startuptask.EnsureSchema(ctx, store)

coordinator := startuptask.NewCoordinator(store, "")
_ = coordinator.RegisterHandler("warm_up_cache", doWarmUp)
spec, _ := startuptask.LoadSpec("/etc/startup/publish_app_settings.yaml")
result, _ := coordinator.Run(ctx, spec)
```

**Rust**

```rust
let store: Arc<dyn Store> = Arc::new(PostgresStore::connect(dsn).await?);
ensure_schema(store.as_ref()).await?;

let mut coordinator = Coordinator::new(store, owner_id);
coordinator.register_handler("warm_up_cache", Box::new(|config| do_warm_up(config)))?;
let spec = load_spec("/etc/startup/publish_app_settings.yaml")?;
let result = coordinator.run(&spec).await;
```

## Task definition YAML

```yaml
task_id: config/publish-app-settings       # required, globally unique, ≤128
version: "2026-10-22-v1"                   # required, ≤64; a version change means re-execution
description: "publish app defaults"        # optional
timeout_seconds: 300                       # optional, default 300, > 0; also the lock lease and the HTTP timeout
execution:
  executor: http                           # http | custom
  config:                                  # fixed shape for http; passed to the handler as-is for custom
    method: PUT                            # required
    url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"   # required, supports ${ENV}
    headers:
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"     # values support ${ENV}
      Content-Type: "application/json"
    body_source: file                      # file | inline | none
    body_path: "app_settings.json"         # required for file (relative to the YAML's directory)
    success_check:
      http_status: [200, 201]              # int or list of ints, default 200
```

```yaml
task_id: cache/warm-up
version: "2026-10-22-v1"
timeout_seconds: 60
execution:
  executor: custom
  handler: warm_up_cache                   # must already be registered in the process
  config:
    keys: ["home:feed", "home:recommend"]
    ttl_seconds: 300
```

| Field | Constraint |
|---|---|
| `task_id` / `version` | Required, non-empty, max length 128 / 64 |
| `timeout_seconds` | Integer, default 300, must be > 0 |
| `execution.executor` | Required, `http` or `custom` |
| `execution.config.method` / `url` | Required when `executor: http` |
| `execution.config.body_path` | Required when `body_source: file`; a relative path resolves against the YAML's directory |
| `execution.handler` | Required when `executor: custom` |
| Unknown fields | Rejected, so a typo cannot silently take effect |

`${VAR}` substitution covers `url`, header values and the inline `body`; a missing variable yields
`FAILED` with `error = environment variable "NAME" is not set`. The full definition is in
[`spec/schema.json`](spec/schema.json).

**Handler naming convention**: names live in the in-process registry, are lower-case snake_case and
globally unique; registering a duplicate name fails immediately. The examples all use
`warm_up_cache`; business code should prefer `<domain>_<action>` (e.g. `iam_sync_permissions`,
`publish_app_settings`, `warm_up_hot_cache`).

## Database

Two tables (full DDL in [`spec/ddl/`](spec/ddl); the SDK provides an idempotent `ensure_schema`):

- `task_execution_lock`: `task_id` primary key, records the current owner, `acquired_at` and
  `expires_at` (the lease).
- `task_version_record`: `(task_id, version)` primary key, records the latest `success` / `failed`.

Time is always UTC and independent of the session time zone (PostgreSQL binds a naive UTC wall
clock, MySQL binds a UTC literal string).

## Tests

Three database tiers, pick as needed:

| Tier | Dependency | Coverage |
|---|---|---|
| SQLite (default) | none | All unit tests, the six coordination scenarios, the 8-way concurrency simulation |
| PostgreSQL | `STARTUP_TASK_PG_DSN` | Dialect SQL, time zones, non-autocommit transactions, 8-way concurrency |
| MySQL / MariaDB | `STARTUP_TASK_MYSQL_DSN` | Dialect SQL, `ON DUPLICATE KEY UPDATE`, `1062`, `DATETIME(6)` precision |

Real-database tests are enabled by environment variables and skip automatically when the
variables are missing; the repository ships no credentials.

> The commands below are written for Windows / Git Bash; on Linux / macOS replace
> `.venv/Scripts/python.exe` with `.venv/bin/python`, everything else stays the same.

```bash
# Python
cd sdk-python && python -m venv .venv && ./.venv/Scripts/python.exe -m pip install -e . pytest
./.venv/Scripts/python.exe -m pytest -q                    # 114 passed, 29 skipped
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
./.venv/Scripts/python.exe -m pytest -q                    # 143 passed

# Go
cd sdk-go && go test ./... -count=1                        # no DSN: ok (integration skipped); with DSN: 93 PASS

# Java
cd sdk-java && mvn -B test                                 # no DSN: 87 run / 21 skipped; with DSN: 87 run

# Rust
cd sdk-rust && cargo test                                  # 72 passed (9 PG + 9 MySQL included when a DSN is set)
```

The coordination algorithm has six scenarios: first execution, repeat execution skipped, concurrent
execution exactly once, expired lock takeover, failure releases the lock, release does not delete
someone else's lock. Concurrency is simulated with "multiple connections + multiple owners + 8-way
concurrency" standing in for multiple pods; no real Kubernetes is involved.

The per-language examples run as-is (`examples/` holds the same set everywhere: a config-center http
task and a cache warm-up custom task, byte-for-byte identical across the four languages):

```bash
cd sdk-python && ./.venv/Scripts/python.exe examples/run_examples.py
cd sdk-go && go run ./examples
cd sdk-rust && cargo run --example run_examples
cd sdk-java && mvn -q -B test-compile \
  dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=target/cp.txt && \
  java -cp "target/classes;$(cat target/cp.txt)" io.startuptask.examples.ExampleRunner
```

## Verified

- All four test suites green (with real databases): Python 143, Go 93 PASS, Java 87, Rust 72.
- All three dialects verified against real databases: PostgreSQL 14.6 (session time zone `PRC`),
  MariaDB 10.4.24 (session `+08:00`) and SQLite; time values are correct to the microsecond and do
  not drift with the session time zone.
- **Cross-language interop on one database**: with the same PG / MySQL database and the same YAML,
  after Python executes and writes success, Go / Java / Rust all return `SKIPPED_ALREADY_DONE`; an
  expired lock can be taken over by another language.
- All 8 example YAML files validate against `spec/schema.json`.
- Not covered: a real multi-replica Kubernetes deployment (replaced by concurrency simulation as
  agreed), MySQL 8-specific verification.

## Known trade-offs

1. **Expiry takeover can execute twice**: once the lease expires a new pod may take over while the
   original pod is still running. Converge with `timeout_seconds` (≥ p99 duration + margin) and
   idempotent handlers.
2. **The crash window is not absolutely deduplicated**: being killed between a successful execution
   and the success write means a rerun.
3. **The lease depends on pod clocks**: NTP is required, or set the lease conservatively.
4. **A failed release leaves the lock behind**: the outcome is still `EXECUTED` and other pods wait
   for expiry.
5. **`version` is the only idempotency key**: changing the content requires a version bump.
6. **HTTP client injectability is uneven**: Java can inject an `HttpClient`, while Go / Python /
   Rust use built-in clients, so proxy / mTLS setups need an extension.
7. **No built-in metrics or CI configuration**: logging only; wiring this up requires a real
   database in the pipeline.

The full list with risk analysis is in [`docs/design.md`](docs/design.md) §11.

## License

MIT, see [`LICENSE`](LICENSE).
