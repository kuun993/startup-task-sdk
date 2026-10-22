English: [design.md](design.md)

# 启动任务协调 SDK 总体设计

> 本文是四种语言实现（Python / Go / Java / Rust）的共同设计基准。文中所有行为约定是
> **规范性的**：任何实现与本文冲突，以本文为准，并同步修正另外三种语言。

## 1. 概述与目标

### 1.1 问题

Kubernetes 部署里，多副本同时启动，每个副本都要做一批初始化动作（发布默认配置到配置中心、
预热热点缓存、同步用户权限等）。这类动作有两个硬约束：

1. **不能并发执行**：两个副本同时发布同一份配置会互相覆盖、甚至产生重复资源；
2. **同一批次只应成功执行一次**：即使后续有副本重启、滚动发布，也不该重复执行。

### 1.2 方案

把「要不要执行」的协调逻辑收敛进一个 SDK，执行什么由调用方用 YAML 声明：

```
查版本 → 抢执行权 → 执行 → 记录结果 → 释放
```

协调状态放在**应用自己的关系型数据库**（PostgreSQL / MySQL，同时支持 SQLite 用于测试与嵌入），
不引入 Redis、ZooKeeper 或独立锁服务。四种语言的 SDK 行为完全同构：算法步骤、SQL 语义、
结果枚举、错误收敛规则逐条一致。

### 1.3 设计原则

- **数据库是唯一协调真相**：不依赖进程内状态、不依赖文件系统、不依赖网络广播。
- **原子性交给数据库**：抢锁只有「主键冲突」和「条件 UPDATE 命中」两条路径，禁止「先查再插」。
- **只删自己的锁**：释放必须带 `owner_id`，避免误删接管者的锁。
- **异常不吞锁**：任何退出路径（成功、失败、提前返回、panic）都必须尝试释放锁。
- **行为可跨语言复现**：时间、判定、SQL、结果字符串都不得依赖语言运行时或会话配置。
- **失败可定位**：日志覆盖「抢到/接管/跳过/失败/释放失败」五类事件。

### 1.4 非目标

不实现独立锁服务；不实现 shell / gRPC / database 等其它执行器；不做定时调度；
不做 UI 或管理后台；不为崩溃接管场景下的非幂等操作提供绝对去重保证（见 §11）。

## 2. 架构与公开接口

### 2.1 运行形态

```
K8s 中每个 Pod 启动
        │
        ├── 应用初始化（日志、配置、数据源）
        │
        ├── 构造 Store（PG / MySQL / SQLite，连接由调用方提供）
        ├── 构造 Coordinator（注入 Store、owner_id、可选时钟）
        ├── register_handler(...) × N        # custom 执行器需要
        └── run_all([spec...]) ──┐
                                 │
                     ┌───────────┴────────────┐
                     │      Coordinator       │  协调算法（§5）
                     └───────┬────────┬───────┘
                             │        │
                    ┌────────┴──┐  ┌──┴────────────┐
                    │   Store   │  │   Executor    │
                    │ 方言 SQL  │  │ http / custom │
                    └────┬──────┘  └───────────────┘
                         │
                 应用数据库（两张表）
```

组件职责：

| 组件 | 职责 | 不负责 |
|---|---|---|
| `Coordinator` | 协调流程、结果收敛、释放锁、日志 | 具体任务逻辑、连接池 |
| `Store` | 五条参数化 SQL、方言差异、时间绑定、冲突识别 | 重试、事务编排 |
| `HttpExecutor` | 发一次 HTTP 请求、状态码判定、环境变量替换 | 重试、响应体解析 |
| `Handler`（custom） | 调用方注册的 `name → fn` | 被 SDK 关心内部实现 |
| `TaskSpec` | YAML 解析与校验的产物（不可变） | 运行时状态 |

### 2.2 公开接口

| 能力 | Python | Java | Go | Rust |
|---|---|---|---|---|
| 初始化 | `Coordinator(store, owner_id=None, clock=None)` | `new Coordinator(store, ownerId[, clock])` | `NewCoordinator(store, ownerID)` / `NewCoordinatorWithClock` | `Coordinator::new(store, owner)` / `with_clock` |
| 注册 handler | `register_handler(name, fn)` | `registerHandler(name, handler)` | `RegisterHandler(name, fn)` | `register_handler(name, f)` |
| 解析定义 | `load_spec(text_or_path)` | `TaskSpecLoader.load(path_or_text)` | `LoadSpec(src)` | `load_spec(src)` |
| 单任务 | `run(spec)` | `run(spec)` | `Run(ctx, spec)` | `run(&spec).await` |
| 批量 | `run_all(specs)` | `runAll(specs)` | `RunAll(ctx, specs)` | `run_all(&specs).await` |
| 建表 | `ensure_schema(store)` | `Schema.ensure(store)` | `EnsureSchema(ctx, store)` | `ensure_schema(&store).await` |

约定：

- `register_handler` 重名必须报错（Python `ValueError` / Java `IllegalStateException` /
  Go `error` / Rust `Err`），避免不同任务悄悄覆盖同一个名字。
- `owner_id` 是执行者身份（Pod 名或 UUID）。未显式传入时默认为 `<hostname>/<8 位 hex>`，
  **进程内固定不变**——否则释放锁时匹配不到自己。
- `run` 对调用方不抛业务异常：一切失败收敛进返回值（唯一例外见 §7.4）。
- `load_spec` 的解析/校验错误是配置错误，直接抛出（不是 `RunResult`）。

## 3. 结果模型

### 3.1 Outcome

| 值 | 含义 | 何时产生 |
|---|---|---|
| `EXECUTED` | 本次拿到了执行权，且执行成功 | 版本未记录、抢锁成功、执行成功、写 success 成功 |
| `SKIPPED_ALREADY_DONE` | 该 `(task_id, version)` 已成功执行过 | 步骤 1 或步骤 3 命中 `status='success'` |
| `SKIPPED_LOCKED` | 没拿到执行权 | 锁被未过期的其他 owner 持有；或接管条件更新影响 0 行 |
| `FAILED` | 拿到了执行权但执行失败（或执行前置条件不满足） | handler 抛错、HTTP 非预期状态码、环境变量缺失、写库失败等 |

`outcome.to_string()` 一律是上表的大写常量名，四语言一致，便于统一打点与告警。
`EXECUTED` **只表示成功**——不存在「执行了但失败」还叫 `EXECUTED` 的情况。

### 3.2 RunResult

| 字段 | 说明 |
|---|---|
| `task_id` / `version` | 取自 spec |
| `outcome` | §3.1 |
| `error` | 仅 `FAILED` 非空；内容是底层错误的原文（不加语言特定前缀） |
| `duration_ms` | 从进入 `run` 到最后一次释放锁尝试结束的墙钟耗时 |

## 4. 数据模型与方言

### 4.1 表结构

```sql
-- 执行权锁表：同一 task_id 同时只有一个 owner
CREATE TABLE task_execution_lock (
    task_id      VARCHAR(128) PRIMARY KEY,
    owner_id     VARCHAR(128) NOT NULL,
    acquired_at  TIMESTAMP    NOT NULL,
    expires_at   TIMESTAMP    NOT NULL
);

-- 版本记录表：(task_id, version) 唯一
CREATE TABLE task_version_record (
    task_id      VARCHAR(128) NOT NULL,
    version      VARCHAR(64)  NOT NULL,
    status       VARCHAR(16)  NOT NULL,      -- success | failed
    executed_at  TIMESTAMP    NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    PRIMARY KEY (task_id, version)
);
```

设计说明：

- 锁表以 `task_id` 作主键，**主键本身就是互斥点**：并发 INSERT 只可能一个成功，其余拿到冲突错误。
  因此不需要 `SELECT ... FOR UPDATE`、不需要长事务、不需要 advisory lock。
- 版本表只保留每个 `(task_id, version)` 的最新一条结果（upsert），不保留执行历史；
  历史审计不在本 SDK 范围内（需要时由调用方另行记录）。
- 两张表由调用方在目标库建好，SDK 提供幂等的 `ensure_schema`（`CREATE TABLE IF NOT EXISTS`）；
  不做 schema 迁移。完整 DDL 见 `spec/ddl/{postgres,mysql,sqlite}.sql`。
- 时间统一 UTC。SQLite 方言把时间存为 `YYYY-MM-DD HH:MM:SS.ffffff` 文本（字典序即时间序）。

### 4.2 SQL

所有语句**参数化**，且 SQL 中不出现 `now()` / `CURRENT_TIMESTAMP`：当前时间由 SDK 时钟算出后
作为绑定参数传入。这样四种语言的判定完全一致，测试也可注入时钟。

```sql
-- Q1 版本检查（存在即视为已完成）
SELECT 1 FROM task_version_record
 WHERE task_id = ? AND version = ? AND status = 'success'

-- Q2 抢锁（主键冲突 = 已被占用）
INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at)
VALUES (?, ?, ?, ?)

-- Q3 过期接管（条件更新，原子）
UPDATE task_execution_lock
   SET owner_id = ?, acquired_at = ?, expires_at = ?
 WHERE task_id = ? AND expires_at < ?

-- Q4 写版本记录（upsert）
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

-- Q5 释放（只删自己的锁）
DELETE FROM task_execution_lock WHERE task_id = ? AND owner_id = ?
```

MySQL 的补充规则：`UPDATE` 默认返回「实际改变的行数」，若接管写入的值与库里完全相同
（同一时钟、同一 owner 重复接管）可能返回 0。因此 MySQL 方言在 Q3 返回 0 时追加一次
参数化回读，确认 `owner_id` 与 `expires_at` 都是本次写入值才算抢到；PostgreSQL / SQLite
直接以影响行数判定。

### 4.3 主键冲突识别

抢锁必须区分「主键冲突（锁被占）」和「其他数据库错误（真故障）」，前者走接管分支，
后者返回 `FAILED` 且不写版本记录。这是方言差异，由方言层封装：

| 方言 | 判定 |
|---|---|
| PostgreSQL | SQLState `23505` |
| MySQL / MariaDB | vendor code `1062`（SQLState `23000` 过宽，不单独使用） |
| SQLite | extended code `1555` / `2067`（或 SQLState `23505`） |

禁止使用 `INSERT IGNORE` / `INSERT OR IGNORE`：它们会把连接错误、字段超长等真故障也吞成「没抢到」。

### 4.4 时间绑定与事务边界

**时间必须绑定为「无时区的 UTC 墙钟」**，且与会话/连接池的时区设置无关：

| 方言 | 绑定形式 |
|---|---|
| PostgreSQL | 无时区 `TIMESTAMP`：Python naive `datetime`、Java `Timestamp.from(Instant)`（显式配 UTC `Calendar`）、Go UTC `time.Time`、Rust `NaiveDateTime` |
| MySQL | `DATETIME(6)`：绑定 UTC 字面量文本 `YYYY-MM-DD HH:MM:SS.ffffff`；Rust 可直接 `NaiveDateTime` |
| SQLite | TEXT，同上格式 |

不这样做的后果（都已验证）：绑定 tz-aware 值会被会话时区（如 `PRC`）偏移 8 小时；
MySQL 驱动会按连接 `loc` 换算时间、并在 MariaDB 上丢掉 `DATETIME(6)` 的小数秒。
一旦偏移，`expires_at < now` 的判定就取决于每个 Pod 的会话时区，等于破坏了互斥语义。

**事务边界**：SDK 不把 Q1–Q5 组合进一个事务，一条语句就是一个工作单元。

- 若调用方提供的连接 `autoCommit=false`，SDK 在每条语句成功后提交、失败后回滚；
- 主键冲突后必须回滚再执行 Q3，否则 PostgreSQL 会因事务进入 aborted 状态而报 `25P02`；
- 调用方**不要**用外层大事务包住 SDK 调用。

## 5. 协调算法

### 5.1 时序

```
Pod A                        DB（锁表 / 版本表）                       Pod B
  │                                   │                                 │
  │ 1. Q1 版本检查                     │                                 │
  │──────────────────────────────────>│                                 │
  │         不存在 → 继续               │                                 │
  │ 2. Q2 INSERT 抢锁(owner=A)         │                                 │
  │──────────────────────────────────>│                                 │
  │          成功 → 持有锁              │<──── 2'. Q2 INSERT(owner=B)      │
  │ 3. Q1 双重检查                     │      主键冲突 → 看 expires_at    │
  │──────────────────────────────────>│      未过期 → SKIPPED_LOCKED     │
  │          不存在 → 继续              │      已过期 → Q3 接管(rows=1)    │
  │ 4. 执行（http / custom handler）    │                                 │
  │════════════ 执行中 ════════════════│                                 │
  │ 5. Q4 upsert success               │                                 │
  │──────────────────────────────────>│                                 │
  │ 6. Q5 DELETE WHERE owner=A         │                                 │
  │──────────────────────────────────>│                                 │
```

### 5.2 伪码

```
run(spec):
  t0 = monotonic_now()
  outcome = null; err = null; lock_held = false
  try:
    # 步骤 1：查版本
    if exists(Q1(task_id, version)):                 # status = 'success'
        outcome = SKIPPED_ALREADY_DONE; return

    # 步骤 2：抢执行权（两条原子路径，禁止「先查再插」）
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

    # 步骤 3：双重检查（接管场景下原 owner 可能刚写完 success）
    if exists(Q1(task_id, version)):
        outcome = SKIPPED_ALREADY_DONE; return         # 仍会走 finally 释放自己的锁

    # 步骤 4 / 4a：执行并记录
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
        catch e: log_warn(...)                         # 释放失败不改变 outcome
  return RunResult(task_id, version, outcome, err, monotonic_now() - t0)
```

### 5.3 关键约束

1. 步骤 1 只认 `status='success'`；`failed` 记录不阻止重试。
2. 抢锁只有 Q2（主键冲突）与 Q3（条件更新命中）两条路径，都依赖数据库原子性。
3. 抢锁失败返回 `SKIPPED_LOCKED`，不是 `FAILED`。
4. 步骤 3 命中返回 `SKIPPED_ALREADY_DONE`（不是 `EXECUTED`，也不是 `SKIPPED_LOCKED`）。
5. 同一轮抢锁只取一次 `now`，`acquired_at` 与 `expires_at` 基于同一个时间点。
6. Q5 必须带 `owner_id`，且必须在 finally / defer / 等价收尾中执行。
7. 释放失败只记日志：版本记录已落库时结果仍是 `EXECUTED`，残留的锁由租约过期回收。

## 6. 任务定义

任务定义是一份 YAML，整体结构如下（顶层字段的完整约束见 §6.4）：

```yaml
task_id: config/publish-app-settings    # 必填，全局唯一，≤128
version: "2026-10-22-v1"                 # 必填，≤64；同一 task_id 下版本变化即新批次
description: "发布应用默认配置"           # 选填（允许空串）
timeout_seconds: 300                     # 选填，整数，默认 300，必须 > 0
execution:
  executor: http                         # http | custom
  config: { ... }                        # http 时结构固定；custom 时原样传给 handler
```

### 6.1 http 执行器

```yaml
execution:
  executor: http
  config:
    method: POST                      # 必填
    url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"   # 必填
    headers:                          # 选填，值支持 ${ENV}
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"
      Content-Type: "application/json"
    body_source: file                 # file | inline | none（默认 none）
    body_path: "app_settings.json"    # file 时必填；相对路径基于 YAML 所在目录
    body: '{"a":1}'                   # inline 时使用，支持 ${ENV}
    success_check:
      http_status: [200, 201]        # int 或 int 列表，默认 200
```

成功判定只看状态码是否落在 `success_check.http_status`。失败信息格式固定：
`http status 500 not in [200] (body: ...)`，传输层错误为 `http request failed: <原因>`。
响应体仅用于错误信息，截断后拼接。

### 6.2 custom 执行器

```yaml
execution:
  executor: custom
  handler: "warm_up_cache"            # 必填，调用方已注册
  config: { keys: [home:feed], ttl_seconds: 300 }   # 选填，深拷贝后原样传给 handler；缺省空 map
```

handler 正常返回 = 成功；抛异常 / 返回非 nil error / 返回 `Err` = 失败。
handler 未注册 → `FAILED`，`error = handler "xxx" is not registered`。

### 6.3 环境变量替换

对 `url`、`headers` 的值、inline `body` 做 `${NAME}` → 环境变量替换，在执行前进行
（不是解析期，便于同一份 spec 在不同环境复用）。变量缺失 → `FAILED`，
`error = environment variable "NAME" is not set`。`body_path` 不做替换。

### 6.4 字段校验规则

| 规则 | 说明 |
|---|---|
| 必填 | `task_id`、`version`、`execution`、`execution.executor` |
| 长度 | `task_id` ≤ 128、`version` ≤ 64，均非空 |
| 未知字段 | 拒绝（避免拼写错误静默生效） |
| `timeout_seconds` | 整数，默认 300，必须 > 0；同时是锁租约与 HTTP 请求超时。计算租约时夹到 100 年上限（防时间类型溢出），校验仍只看 `> 0` |
| `executor` | `http` / `custom`，大小写敏感，空串非法 |
| `execution.config.method` / `url` | `executor: http` 时必填 |
| `execution.config.body_path` | `body_source: file` 时必填；空串视为缺失 |
| `execution.handler` | `executor: custom` 时必填 |
| 空串 | `description` 与 inline `body` 允许空串 |
| 错误信息 | 必须含字段路径：`task.timeout_seconds must be > 0`、`execution.config.method is required` |

JSON Schema 见 `spec/schema.json`，四语言解析器与之对齐。

## 7. 错误收敛、异常与并发语义

### 7.1 分阶段处理

| 阶段 | 失败处理 |
|---|---|
| 步骤 1（版本检查） | 数据库异常 → `FAILED`，尝试写 `failed` 记录，不持有锁 |
| 步骤 2（抢锁） | 非冲突类异常 → `FAILED`，**不写**版本记录、**不**释放（尚未持有锁） |
| 步骤 3（双重检查） | 数据库异常 → `FAILED`，写 `failed`，释放自己的锁 |
| 步骤 4（执行） | 任何失败 → `FAILED`，写 `failed`，释放锁 |
| 失败记录写入 | 写不进去只记 warning，不改变 outcome |
| 释放锁 | 失败只记 warning，不改变 outcome |

### 7.2 「任何异常」的边界

Python / Go / Rust 把执行阶段的任意异常（含 Go panic、Rust panic）收敛为 `FAILED`，
并保证写 `failed` 与释放锁。Java 捕获 `Throwable`：普通 `Error`（如 `AssertionError`）同样
转 `FAILED`；`VirtualMachineError`（OOM / StackOverflow）记日志后重抛，但 `finally` 仍释放锁——
堆已耗尽时继续建连接写库不可靠，交给容器重启更稳。这是四语言里唯一的刻意差异。

### 7.3 同进程并发与 batch

- `run_all` 串行执行，结果与入参同序，单个失败不影响后续。
- 同一进程内并发调用 `run` 同一 `(task_id, version)`：先到的执行，后到的因锁未过期得到
  `SKIPPED_LOCKED`（owner 相同但 INSERT 仍冲突）。语义安全，但同一进程没有理由这样做。
- `Coordinator` 的 handler 注册表只读，`run` 可在多线程/多任务中并发调用（各语言分别用
  并发容器、锁或 `&self` 保证）。Store 的连接使用方式由实现决定：Python 为单连接 + 互斥锁
  （启动任务量小，够用），Go/Java/Rust 由调用方提供连接池。

### 7.4 不抛异常的例外

Java 的 `VirtualMachineError` 会重抛；Python 的 `KeyboardInterrupt` / `SystemExit` 透传。
其余情况 `run` 都返回 `RunResult`。

## 8. 日志与可观测性

统一事件：`lock acquired`、`lock takeover`、`skip already done`、`skip locked`、
`task failed`、`release failed`。日志内容不参与任何行为判定。

字段建议（各语言按自身日志设施输出）：`task_id`、`version`、`owner_id`、`outcome`、
`duration_ms`、错误原文。当前只到日志层，内建指标/追踪不在本版本范围（见 §11）。

## 9. 使用方式

四语言的初始化与注册方式见各自目录的 README（`sdk-python/`、`sdk-go/`、`sdk-java/`、
`sdk-rust/`），共同形态：

```text
store = <PostgresStore | MysqlStore | SqliteStore>(连接信息)
ensure_schema(store)
coordinator = Coordinator(store, owner_id)      # 或省略 owner，用默认的 <hostname>/<hex>
coordinator.register_handler("warm_up_cache", fn)
results = coordinator.run_all(load_spec("...").for each spec)
```

示例任务（每种语言的 `examples/` 下都有，四份逐字节一致）：
- `publish_app_settings.yaml` + `app_settings.json`：http 执行器，`PUT` 到配置中心，
  header 用 `${CONFIG_CENTER_TOKEN}`，body 来自文件，成功码 `[200, 201]`；
- `warm_up_cache.yaml`：custom 执行器，handler `warm_up_cache`，config 为 `keys` + `ttl_seconds`。

**发布纪律**：`version` 是唯一的幂等键。改了 YAML 内容（url、body、参数）必须同时 bump
`version`，否则新内容不会被执行。

## 10. 验证与测试矩阵

### 10.1 三层测试

| 层 | 内容 | 依赖 |
|---|---|---|
| 单元 | YAML 校验、环境变量替换、状态码判定、方言 SQL/冲突识别、时间格式 | 无外部依赖 |
| 场景模拟 | 用多连接、多 owner 模拟多 Pod，覆盖 §10.2 六个场景 | SQLite（文件库，WAL） |
| 真库 | 同场景 + 方言专属断言，跑 PostgreSQL / MySQL | 真实数据库 |

场景模拟用 8 个并发调用方各持独立连接，断言「恰好 1 个 EXECUTED、其余 SKIPPED_LOCKED」，
以此替代真实 K8s 多副本压测。真实 K8s 部署不在验证范围。

### 10.2 必测场景

| # | 场景 | 断言 |
|---|---|---|
| 1 | 单 Pod 首次执行 | `EXECUTED`，版本记录 success，锁表清空 |
| 2 | 重复执行 | `SKIPPED_ALREADY_DONE`，且运行前后锁表都为空（没抢锁） |
| 3 | 并发抢锁 | 8 并发 → 恰好 1 `EXECUTED`、7 `SKIPPED_LOCKED`，handler 只执行 1 次 |
| 4 | 锁过期接管 | 预置过期锁 → 新 Pod `EXECUTED`，锁 owner 变为新 Pod |
| 5 | 执行失败 | `FAILED`，锁释放，版本记录为 `failed`（重试可再次 `EXECUTED`） |
| 6 | 释放不误删 | A 的锁被 B 接管后，A 的释放不影响 B 的锁 |

补充覆盖：抢锁阶段的非冲突错误（`FAILED` 且不写记录不释放）、释放失败不改变 outcome、
抢锁后双重检查命中、时钟注入下的租约边界与耗时、HTTP 执行器（本地 mock server，
含 `${ENV}`、错误文案、超时透传）。

### 10.3 真库结果

| 方言 | 环境 | 覆盖 |
|---|---|---|
| PostgreSQL | 14.6（会话时区 `PRC`） | DDL、Q1–Q5、8 并发、接管、非 autocommit 连接的冲突后接管（无 `25P02`）、时间往返（含 `Asia/Shanghai` 会话） |
| MySQL | MariaDB 10.4.24（会话 `+08:00`） | DDL、`ON DUPLICATE KEY UPDATE`、`1062` 识别、`DATETIME(6)` 微秒、8 并发、接管 |
| SQLite | 本地文件库 | 全部场景与单元测试 |

**跨语言互操作**：在同一个 PG 库、同一个 MySQL 库上，Python 执行并写 success 后，
Go / Java / Rust 都返回 `SKIPPED_ALREADY_DONE`；过期锁可由另一语言接管；四语言写入的时间
值逐微秒一致且与会话时区无关。

### 10.4 运行方式

SQLite 测试开箱即跑；真库测试由 `STARTUP_TASK_PG_DSN` / `STARTUP_TASK_MYSQL_DSN`
开关，缺失时自动跳过（有库环境：Python 143 通过 / Go 93 条 PASS / Java 87 通过 / Rust 72 通过；
无库环境：Python 114 通过 + 29 跳过、Go ok、Java 87 跑 21 跳过、Rust 72 通过）。

## 11. 限制、取舍与风险

1. **接管会带来重复执行**。锁过期后新 Pod 可接管，此时原 Pod 可能仍在执行，于是并发两份。
   这是放弃独立锁服务后的必然取舍：用 `timeout_seconds`（≥ 任务 p99 耗时 + 余量）和幂等
   handler 来收敛。
2. **崩溃窗口不做绝对去重**。「执行成功」与「写 success」之间被杀，下一 Pod 会重跑。
   没有分布式事务就无法消除。
3. **租约依赖各 Pod 的时钟**。时间由 SDK 计算，Pod 间漂移会放大或缩小有效租约；生产需保证 NTP，
   或把租约设得保守。
4. **释放失败会残留锁**。版本记录已落库时结果仍为 `EXECUTED`，残留锁会让其他 Pod 等到过期。
5. **`timeout_seconds` 一值两用**（租约 + HTTP 超时）。任务耗时接近超时时，会出现
   「还在跑但锁已可被接管」。
6. **版本号是唯一幂等键**。只改内容不 bump version 不会重跑；需要发布流程纪律。
7. **MySQL 8 未单独验证**。真库验证用的是 MariaDB 10.4；MySQL 8.0.20+ 对
   `ON DUPLICATE KEY UPDATE ... VALUES(col)` 已标记 deprecated（当前仍可用，未来版本需评估）。
8. **HTTP 客户端可注入性不齐**。Java 可注入 `HttpClient`；Go / Python / Rust 当前使用内置客户端，
   在需要自定义代理、mTLS、自签 CA 的环境需要扩展（否则只能依赖默认的代理环境变量）。
9. **没有内建指标/追踪**，只有日志；多副本同时启动时的排障依赖日志聚合。
10. **没有内建 CI 配置**。真库测试靠环境变量开关，接入时需在流水线里提供 PG / MySQL 服务，
    否则「只有 SQLite 在跑」会掩盖方言、时区、精度类问题。
11. **Go 的 `-race` 未纳入验证**（验证环境无 cgo 工具链）；并发正确性由多连接真库测试保证。
12. **不做**独立锁服务、其他执行器、定时调度、UI、管理后台。

## 12. 演进方向

- HTTP 执行器开放客户端注入（代理 / mTLS / 自定义 CA），四语言对齐；
- 内建指标钩子（执行数、跳过数、失败数、抢锁耗时）与结构化日志字段；
- CI 接入真实 PG / MySQL，方言改动必须在该矩阵上通过；
- 视需要在 MySQL 8 上做一次验证并评估 upsert 语法演进。
