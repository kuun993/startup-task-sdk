English: [README.md](README.md)

# startup-task-sdk（Rust）


Kubernetes 多 Pod 启动时的启动任务协调 SDK。同一 `(task_id, version)` 只应成功执行一次：

```
查版本（Q1）→ 抢执行权（Q2 原子 INSERT / Q3 过期接管）→ 双重检查（Q1）
→ 执行（http / custom）→ 记录结果（Q4 upsert）→ 释放锁（Q5，带 owner_id 条件）
```

行为基准是仓库根目录的 `docs/design.zh-CN.md`（normative），本实现逐条对齐，未做任何行为裁剪。

## 目录

| 路径 | 说明 |
|---|---|
| `src/spec.rs` | `TaskSpec`、`load_spec()`、`SpecError`（YAML 解析 + 手动校验，错误信息带字段路径） |
| `src/store.rs` | `Store` trait、`PostgresStore`、`MysqlStore`、`SqliteStore`、方言 SQL 与冲突识别 |
| `src/executor.rs` | http 执行器（reqwest）、`${VAR}` 替换、错误文案 |
| `src/coordinator.rs` | `Coordinator`、协调算法、`Clock`、释放锁收尾、panic 收敛 |
| `src/outcome.rs` | `Outcome`、`RunResult` |
| `src/schema.rs` | `ensure_schema(&store)` |
| `ddl/{sqlite,postgres,mysql}.sql` | 建表语句（与 `spec/ddl/*.sql` 一致） |
| `examples/` | 可运行的 http / custom 示例 |
| `tests/coordinator.rs` | 契约 §10 的 6 个场景（SQLite 真实 SQL）+ 错误收敛 / 时钟 / panic 用例 |
| `tests/spec.rs` | spec 校验负例、环境变量替换 |

## 初始化

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

// 或 SQLite（仅测试 / 嵌入式）
let sqlite = SqliteStore::connect("sqlite://startup-task.db?mode=rwc").await?;
ensure_schema(&sqlite).await?;
let mut coordinator = Coordinator::new(Arc::new(sqlite) as Arc<dyn Store>, "pod-7f9c");
```

`owner_id` 建议取 Pod 名；不关心时用 `Coordinator::with_generated_owner(store)`，
它生成 `<hostname>/<uuid8>` 并**在进程内固定**（首次生成后缓存，保证 Q5 能匹配到自己）。

`PostgresStore::new(PgPool)` / `MysqlStore::new(MySqlPool)` / `SqliteStore::new(SqlitePool)`
可复用调用方已有的连接池；SQLite 想自定义 `journal_mode=WAL`、`busy_timeout` 时用
`SqliteStore::connect_with_options(SqliteConnectOptions::new()...)`。

### 注入时钟

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

所有写入数据库的时间戳（`acquired_at` / `expires_at` / `executed_at`）与
`RunResult.duration` 都来自这个时钟，因此可在测试里精确断言过期边界与耗时。

## 注册 custom handler

```rust
coordinator.register_handler(
    "warm_up_cache",
    Box::new(|config: serde_json::Value| -> Result<(), startup_task_sdk::HandlerError> {
        let keys = config["keys"].as_array().cloned().unwrap_or_default();
        let ttl_seconds = config["ttl_seconds"].as_u64().unwrap_or(300);
        // ... 真正去预热这些 key，ttl 用 ttl_seconds ...
        Ok(())
    }),
)?;
```

- handler 收到的是 `execution.config`（缺省 `{}`，深拷贝，不含 `handler` 字段）。
- 返回 `Ok(())` = 成功；返回 `Err` = 失败（`RunResult.error` 取该错误的 `Display`）。
- **handler `panic!` 不会穿透 `run()`**：`catch_unwind` 把它转成 `FAILED`，
  `error = 'handler panicked: <msg>'`，同样释放锁并写 `failed` 记录。
- 重名注册返回 `Err(RegisterHandlerError)`。
- handler 未注册：`FAILED`，`error = 'handler "xxx" is not registered'`。

## 执行

```rust
let spec = load_spec("examples/warm_up_cache.yaml")?;   // 文件路径
let result = coordinator.run(&spec).await;
// 或 YAML 文本：load_spec(yaml_str)
println!("{} {} {:?} {:?}", result.outcome.as_str(), result.duration_ms(), result.error, result.task_id);

let results = coordinator.run_all(&specs).await;         // 串行、与入参同序
```

`run()` 不 panic、不返回 `Err`；所有失败收敛到 `Outcome::Failed` + `RunResult.error`。
`Outcome::as_str()` / `Display` 给出大写常量名：`EXECUTED`、`SKIPPED_ALREADY_DONE`、
`SKIPPED_LOCKED`、`FAILED`。

## YAML 字段

| 字段 | 必填 | 说明 |
|---|---|---|
| `task_id` | 是 | 1..128，匹配 `^[A-Za-z0-9._:/-]+$` |
| `version` | 是 | 1..64；变化即视为新批次 |
| `description` | 否 | 说明 |
| `timeout_seconds` | 否 | 默认 300，必须 > 0；同时是 HTTP 请求超时与锁的 `expires_at` 偏移 |
| `execution.executor` | 是 | `http` / `custom`（大小写敏感） |

### `executor: http`

| 字段 | 必填 | 说明 |
|---|---|---|
| `config.method` | 是 | 大写后交给 HTTP 客户端 |
| `config.url` | 是 | 支持 `${ENV}` |
| `config.headers` | 否 | string→string，值支持 `${ENV}` |
| `config.body_source` | 否 | `file` / `inline` / `none`（默认 `none`） |
| `config.body_path` | `body_source=file` 时必填 | 相对路径：spec 由文件加载则相对该文件目录，由字符串加载则相对进程工作目录 |
| `config.body` | 否 | `body_source=inline` 的请求体，支持 `${ENV}` |
| `config.success_check.http_status` | 否 | int 或 int 列表，默认 `200` |

### `executor: custom`

`execution.handler` 必填（非空）；`execution.config` 选填，原样传给 handler。

### 环境变量替换

只对 `url`、`headers` 的值、inline `body` 生效，**在 HTTP 请求前**替换（不是解析期），
`body_path` 不替换。`${NAME}` 中 `NAME` 需匹配 `[A-Za-z_][A-Za-z0-9_]*`，不匹配的占位符原样保留。

### 固定错误文案

| 场景 | `RunResult.error` |
|---|---|
| 环境变量缺失 | `environment variable "NAME" is not set` |
| 状态码不在白名单 | `http status 500 not in [200] (body: ...)` |
| 传输错误 / 超时 | `http request failed: <原因>` |
| handler 未注册 | `handler "xxx" is not registered` |
| handler panic | `handler panicked: <panic 消息>` |

存储层错误（Q1..Q5 的 SQL 错误）**原样透传**到 `RunResult.error`，不加任何前缀。

## 示例

`examples/` 下是两个可运行的场景（四个语言同名同内容）：

| 文件 | 说明 |
|---|---|
| `publish_app_settings.yaml` | `executor: http`：`PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings`，header 用 `${CONFIG_CENTER_TOKEN}`，body 来自 `app_settings.json` |
| `app_settings.json` | 上面任务的请求体（feature flags + limits） |
| `warm_up_cache.yaml` | `executor: custom`：`handler: warm_up_cache`，config 带 `keys` 与 `ttl_seconds` |
| `run_examples.rs` | 注册 handler + SQLite 建库 + `run_all` 两个任务并打印 |

```bash
cd sdk-rust
cargo run -q --example run_examples
```

示例用 SQLite（默认 `%TEMP%/startup-task-sdk-example.db`，可用 `STARTUP_TASK_DB` 覆盖），
注册 `warm_up_cache` 后 `run_all` 两个任务：配置中心地址不可达时 http 任务打印 `FAILED`
（不 panic），custom 任务打印 `EXECUTED`；再跑一次 custom 任务变成 `SKIPPED_ALREADY_DONE`。

```text
store      : sqlite://.../startup-task-sdk-example.db?mode=rwc

== run_all (2 tasks) ==
    warm_up_cache: warming up 3 keys ["home:feed", "home:recommend", "config:feature-flags"], ttl_seconds=300
  FAILED task_id=config/publish-app-settings version=2026-10-22-v1 duration_ms=37 error=<配置中心不可达或返回非 2xx 的原因>
  EXECUTED task_id=cache/warm-up version=2026-10-22-v1 duration_ms=24
```

`CONFIG_CENTER_URL` / `CONFIG_CENTER_TOKEN` / `APP_NAME` 未设置时示例会填演示用默认值
（`http://127.0.0.1:8080` / `example-token` / `demo-app`）。

## 测试

```bash
cd sdk-rust
cargo test
```

- `tests/coordinator.rs`：契约 §10 的 6 个场景（`first_run_executed`、`second_run_skipped_already_done`、
  `concurrent_only_one_executes`、`expired_lock_takeover`、`failure_releases_lock_and_no_success`、
  `release_does_not_delete_other_owner_lock`），外加错误收敛用例（handler 未注册/重名注册/panic、
  `failed` 记录可重试且 success 覆盖、`run_all` 顺序与隔离、http 缺环境变量/传输错误/非预期状态码、
  步骤 3 双重检查命中、Q1 错误原样透传）。每个测试用独立临时库文件，
  `journal_mode=WAL` + `busy_timeout=10s` + `max_connections=4`；并发场景是 **8 个独立 Coordinator**
  （各自 owner_id、共享真实多连接池）竞争；http 执行器用本地 mock server 走真实请求；
  另有两个时钟用例用注入的 `ManualClock` 精确断言过期边界（`expires_at == now` 不算过期）与 `duration`。
- `tests/spec.rs`：spec 校验负例（含未知字段的字段路径）、方法大写归一、环境变量替换。
- `tests/integration_pg.rs` / `tests/integration_mysql.rs`：**真库集成测试**（见下一节）。
- 无 Docker / 无本地数据库也能跑：单元/契约测试全部走 SQLite 方言 + 真实 SQL；
  集成测试在没有 DSN 时自动跳过（打印一行后返回，用例仍计为通过）。

## 集成测试（真库）

真库测试需要两个 DSN 环境变量，不在仓库里保存凭据：

```bash
cd sdk-rust
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
cargo test                              # 有 DSN：跑真库用例
cargo test --test integration_pg        # 只跑 PostgreSQL
cargo test --test integration_mysql     # 只跑 MySQL / MariaDB
```

**跳过机制**：Rust 的测试函数没有内置 skip。每个集成用例先读 DSN，读不到就
`eprintln!("skipping ...: STARTUP_TASK_*_DSN is not set")` 然后 `return`，所以无库环境
`cargo test` 依然全绿（这两个文件各 9 个用例，跳过时耗时几毫秒）。

约束与做法：

- 只用 `ensure_schema` 建表，**不 DROP 数据库**；每个用例的 `task_id` 都带进程号+纳秒唯一后缀
  （`itest.pg.*` / `itest.mysql.*`），结束前 `DELETE` 掉自己那两行，互不干扰、可并行跑
  （断言失败时该用例的行会留在库里，方便事后排查；正常跑完是 0 行）。
- 每个用例在自己的 Tokio 运行时里建池（`static` 池会跨运行时复用，报
  `A Tokio 1.x context was found, but it is being shutdown`）；普通用例 `max_connections = 2`，
  并发用例 `= 8` 并先预热 8 条连接，把 TCP/认证抖动挡在计时窗口外。
- 并发用例的确定性：handler 里用 **`tokio::task::block_in_place`** 持锁等到其余 7 个 Pod 都跑完。
  直接 `std::thread::sleep` 会把这个 worker（可能正持有 I/O driver）卡住，其余任务全部停摆 ——
  实测会稳定卡满 30s。这一点对使用方同样成立：同步 handler 里要阻塞就 `block_in_place`。
- PostgreSQL 上 `CREATE TABLE IF NOT EXISTS` 存在并发竞争，集成测试内部用一把进程级异步锁串行化建表。
- PostgreSQL / MySQL 的时间列精度是微秒：断言「读回 == 下发」前把 `Utc::now()` 截断到微秒
  （SDK 行为不变，`expires_at` 比较也不受影响）。

| 覆盖点 | PG | MySQL |
|---|---|---|
| `ensure_schema` 真建表 + 列类型（PG `VARCHAR(128)`/`TIMESTAMP`，MySQL `VARCHAR(128)`/`DATETIME(6)`）+ 幂等 | ✓ | ✓ |
| 首次 `EXECUTED` + `success` + 锁表清空 | ✓ | ✓ |
| 同 version 再跑 `SKIPPED_ALREADY_DONE`，前后锁表为空 | ✓ | ✓ |
| 8 并发 → 恰好 1 `EXECUTED` / 7 `SKIPPED_LOCKED` / handler 1 次 | ✓ | ✓ |
| 过期锁被接管，handler 内部断言锁 owner 已是新 Pod | ✓ | ✓ |
| handler `Err` → `FAILED` + 锁释放 + `failed` 记录 | ✓ | ✓ |
| A 抢锁后锁被改成 B，A 释放不删 B 的锁 | ✓ | ✓ |
| 未过期的活锁走「锁被占」分支（证明 `23505` / `1062` 被识别） | ✓ | ✓ |
| 会话时区 `Asia/Shanghai` 下时间绑定的读回与比较都不漂 | ✓ | ✓（`+08:00`） |
| `ON DUPLICATE KEY UPDATE` 只留一行并覆盖（failed → success） | — | ✓ |

## 已知取舍

1. **接管会导致重复执行。** 锁过期（`expires_at < now`）后允许任何 Pod 接管，原 owner 可能仍在执行，
   于是同一 `(task_id, version)` 可能被执行两次 —— 这是契约 §5.3 接受的取舍，非幂等任务需自己在
   执行侧保证幂等，或把 `timeout_seconds` 设为大于最坏执行耗时。
2. **只看 `status = 'success'` 去重。** `failed` 记录不阻止重试；重跑失败任务会覆盖原记录（Q4 upsert）。
3. **释放锁失败只记日志。** 不影响 `Outcome`，锁会在 `expires_at` 后自然过期被接管。
4. **只有抢锁阶段（Q2/Q3）的数据库错误不写版本记录、不释放锁**（契约 §7）：此时无法安全判断
   是否持有锁。步骤 1/3 的 Q1 错误与执行阶段错误一律写一条 `status='failed'` 记录。
5. **MySQL 冲突识别只认 vendor code `1062`**（契约 §4.3）；SQLite 认扩展码 `1555`/`2067`
   并用 SQLState `23505` 兜底；PostgreSQL 只认 SQLState `23505`。不做「按错误文本猜」的兜底。
6. **handler 是同步函数**，在 async 任务里直接调用；handler 的 panic 会被 `catch_unwind`
   收敛为 `FAILED`（并写 `failed` 记录、释放锁）。
   需要真正阻塞（sleep / 等锁 / 重 CPU）时，请用 `tokio::task::block_in_place(|| ...)`
   （multi-thread 运行时）或把工作丢到 `spawn_blocking`：裸 `std::thread::sleep` 会卡住 worker，
   极端情况下连带卡住 runtime 的 I/O driver，让同进程里其它任务全部停摆。
7. **SQLite 时间以 TEXT 存储**（`YYYY-MM-DD HH:MM:SS.ffffff`，UTC，6 位小数），字典序即时间序；
   所有时间均为 UTC，不做本地时区转换。
8. **`timeout_seconds` 在算 `expires_at` 时夹到 100 年上限**，避免极端值让 `chrono` 溢出
   （`run` 不允许 panic）；校验语义不变（仍只要求 `> 0`）。
