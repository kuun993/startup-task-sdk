English: [README.md](README.md)

# 启动任务协调 SDK（Java / Python / Go / Rust）

Kubernetes 多副本启动时，初始化动作（发布默认配置到配置中心、预热热点缓存、同步用户权限……）**不能并发执行**，
同一个版本也应该**只成功执行一次**。本仓库提供四种语言同构的 SDK：把「查版本 → 抢执行权 →
执行 → 记录结果 → 释放锁」这段协调逻辑收敛成一份实现，执行什么由调用方用 YAML 声明。

- 协调状态放在**应用自己的关系型数据库**（PostgreSQL / MySQL，SQLite 用于测试与嵌入式），
  不引入 Redis 或独立锁服务。
- 四种语言共用一份设计基准：[`docs/design.zh-CN.md`](docs/design.zh-CN.md)，算法步骤、SQL、结果枚举、
  错误收敛规则逐条一致。
- 过期接管导致的重复执行是**设计上接受的取舍**，见「已知取舍」。

## 目录结构

```
startup-task-sdk/
├── README.md                  ← 英文概览
├── README.zh-CN.md            ← 本文件（中文概览）
├── LICENSE                    ← MIT
├── docs/
│   ├── design.zh-CN.md        ← 总体设计（算法、数据模型、实现约定、验证矩阵）
│   └── design.md              ← overall design (English)
├── spec/
│   ├── schema.json            ← 任务定义的 JSON Schema
│   └── ddl/{postgres,mysql,sqlite}.sql
├── sdk-python/                ← pip 包 startup_task
├── sdk-go/                    ← package startuptask
├── sdk-java/                  ← Maven 项目 io.startuptask:startup-task-sdk
└── sdk-rust/                  ← crate startup-task-sdk
```

每种语言目录下都有自己的 README（英文 `README.md` / 中文 `README.zh-CN.md`）：初始化、handler 注册、YAML 说明、测试命令、实现细节。

## 核心概念

| 概念 | 说明 |
|---|---|
| Task | 一个启动时执行的初始化动作，`task_id` 全局唯一 |
| Version | 执行批次；库里存在 `(task_id, version, success)` 就跳过 |
| Owner | 抢到锁的执行者（Pod 名或 UUID，进程内固定） |
| Executor | `http`（SDK 内置）或 `custom`（调用方注册的 handler） |
| Outcome | `EXECUTED` / `SKIPPED_ALREADY_DONE` / `SKIPPED_LOCKED` / `FAILED` |

`RunResult` 四种语言字段同义：`task_id`、`version`、`outcome`、`error`、`duration_ms`；
`outcome` 的字符串形式统一为大写常量名，便于打点与告警。

## 协调流程

```
Pod A                        DB（锁表 / 版本表）                       Pod B
  │ 1. 查版本（success?）              │                                 │
  │──────────────────────────────────>│                                 │
  │        不存在 → 继续                │                                 │
  │ 2. INSERT 锁(owner=A, now, 租约)   │                                 │
  │──────────────────────────────────>│<─── INSERT 锁(owner=B) 主键冲突  │
  │        成功 → 持有锁                │     未过期 → SKIPPED_LOCKED      │
  │ 3. 再查一次版本（双重检查）          │     已过期 → 条件 UPDATE 接管     │
  │──────────────────────────────────>│                                 │
  │ 4. 执行（http 请求 / custom handler）                                 │
  │ 5. UPSERT 版本记录 = success        │                                 │
  │──────────────────────────────────>│                                 │
  │ 6. DELETE 锁 WHERE owner=A（finally）                                │
  │──────────────────────────────────>│                                 │
```

三条不可动摇的规则：

1. 抢锁只用两种原子操作：`INSERT` 主键冲突、`UPDATE ... WHERE expires_at < ?` 影响行数为 1。
   **禁止先查再插**。
2. 释放锁必须带 `owner_id`，且在任何退出路径（成功、失败、panic、提前返回）都执行。
3. SQL 里不出现数据库时间函数：当前时间由 SDK 时钟算好后作为参数传入，四语言判定一致，
   测试可注入时钟。

## 快速开始

API 名称对照：

| 能力 | Python | Java | Go | Rust |
|---|---|---|---|---|
| 初始化 | `Coordinator(store, owner_id=...)` | `new Coordinator(store, ownerId)` | `NewCoordinator(store, ownerID)` | `Coordinator::new(Arc<dyn Store>, owner)` |
| 注册 handler | `register_handler(name, fn)` | `registerHandler(name, lambda)` | `RegisterHandler(name, fn)` | `register_handler(name, Box::new(f))` |
| 解析定义 | `load_spec(text_or_path)` | `TaskSpecLoader.load(...)` | `LoadSpec(...)` | `load_spec(path)` |
| 单任务 | `run(spec)` | `run(spec)` | `Run(ctx, spec)` | `run(&spec).await` |
| 批量 | `run_all(specs)` | `runAll(specs)` | `RunAll(ctx, specs)` | `run_all(&specs).await` |
| 建表 | `ensure_schema(store)` | `Schema.ensure(store)` | `EnsureSchema(ctx, store)` | `ensure_schema(&store).await` |

**Python**

```python
from startup_task import Coordinator, PostgresStore, ensure_schema, load_spec

store = PostgresStore("postgresql://app:secret@db:5432/app")
ensure_schema(store)

coordinator = Coordinator(store)                  # owner_id 默认 <hostname>/<uuid8>
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
store := startuptask.NewPostgres(db)              // db 由调用方 sql.Open("postgres", dsn)
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

## 任务定义 YAML

```yaml
task_id: config/publish-app-settings      # 必填，全局唯一，≤128
version: "2026-10-22-v1"                   # 必填，≤64；改 version 即重新执行
description: "发布应用默认配置"             # 选填
timeout_seconds: 300                       # 选填，默认 300，> 0；同时作为锁租约与 HTTP 超时
execution:
  executor: http                           # http | custom
  config:                                  # http 时结构固定；custom 时原样传给 handler
    method: PUT                            # 必填
    url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"   # 必填，支持 ${ENV}
    headers:
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"     # 值支持 ${ENV}
      Content-Type: "application/json"
    body_source: file                      # file | inline | none
    body_path: "app_settings.json"         # file 时必填（相对 YAML 所在目录）
    success_check:
      http_status: [200, 201]              # int 或 int 列表，默认 200
```

```yaml
task_id: cache/warm-up
version: "2026-10-22-v1"
timeout_seconds: 60
execution:
  executor: custom
  handler: warm_up_cache                   # 必须已在进程内注册
  config:
    keys: ["home:feed", "home:recommend"]
    ttl_seconds: 300
```

| 字段 | 约束 |
|---|---|
| `task_id` / `version` | 必填，非空，长度上限 128 / 64 |
| `timeout_seconds` | 整数，默认 300，必须 > 0 |
| `execution.executor` | 必填，`http` 或 `custom` |
| `execution.config.method` / `url` | `executor: http` 时必填 |
| `execution.config.body_path` | `body_source: file` 时必填；相对路径基于 YAML 所在目录 |
| `execution.handler` | `executor: custom` 时必填 |
| 未知字段 | 拒绝，避免拼写错误静默生效 |

`${VAR}` 替换范围：`url`、`headers` 的值、inline `body`；缺失 → `FAILED`，
`error = environment variable "NAME" is not set`。完整定义见 [`spec/schema.json`](spec/schema.json)。

**handler 命名约定**：进程内注册表的名字，小写 snake_case、全局唯一，重名注册直接报错。
示例统一用 `warm_up_cache`；业务侧建议 `<领域>_<动作>`（如 `iam_sync_permissions`、
`publish_app_settings`、`warm_up_hot_cache`）。

## 数据库

两张表（完整 DDL 见 [`spec/ddl/`](spec/ddl)，SDK 提供幂等的 `ensure_schema`）：

- `task_execution_lock`：`task_id` 主键，记录当前 owner、`acquired_at`、`expires_at`（租约）。
- `task_version_record`：`(task_id, version)` 主键，记录最近一次 `success` / `failed`。

时间统一 UTC，且与会话时区无关（PostgreSQL 绑无时区 UTC 墙钟，MySQL 绑 UTC 字面量文本）。

## 测试

三档数据库，按需选择：

| 档位 | 依赖 | 覆盖 |
|---|---|---|
| SQLite（默认） | 无 | 全部单元测试、六个协调场景、8 路并发模拟 |
| PostgreSQL | `STARTUP_TASK_PG_DSN` | 方言 SQL、时区、非 autocommit 事务、8 路并发 |
| MySQL / MariaDB | `STARTUP_TASK_MYSQL_DSN` | 方言 SQL、`ON DUPLICATE KEY UPDATE`、`1062`、`DATETIME(6)` 精度 |

真库测试由环境变量开关，缺失时自动跳过；仓库不内置任何凭据。

> 下面的命令按 Windows / Git Bash 书写；Linux / macOS 把 `.venv/Scripts/python.exe`
> 换成 `.venv/bin/python` 即可，其余命令相同。

```bash
# Python
cd sdk-python && python -m venv .venv && ./.venv/Scripts/python.exe -m pip install -e . pytest
./.venv/Scripts/python.exe -m pytest -q                    # 114 passed, 29 skipped
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
./.venv/Scripts/python.exe -m pytest -q                    # 143 passed

# Go
cd sdk-go && go test ./... -count=1                        # 无 DSN：ok（集成跳过）；有 DSN：93 PASS

# Java
cd sdk-java && mvn -B test                          # 无 DSN：87 run / 21 skipped；有 DSN：87 run

# Rust
cd sdk-rust && cargo test                                  # 72 passed（有 DSN 时含 9 PG + 9 MySQL）
```

协调算法六个场景：首次执行、重复执行跳过、并发只执行一次、过期锁接管、失败释放锁、
释放不误删他人锁。并发用「多连接 + 多 owner + 8 路并发」模拟多 Pod，不依赖真实 Kubernetes。

各语言示例可直接运行（`examples/` 下是同一组示例：配置中心 http 任务 + 缓存预热 custom 任务，四份逐字节一致）：

```bash
cd sdk-python && ./.venv/Scripts/python.exe examples/run_examples.py
cd sdk-go && go run ./examples
cd sdk-rust && cargo run --example run_examples
cd sdk-java && mvn -q -B test-compile \
  dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=target/cp.txt && \
  java -cp "target/classes;$(cat target/cp.txt)" io.startuptask.examples.ExampleRunner
```

## 已验证

- 四套测试全绿（有真库时）：Python 143、Go 93 条 PASS、Java 87、Rust 72。
- 三种方言分别在真库上验证：PostgreSQL 14.6（会话时区 `PRC`）、MariaDB 10.4.24（会话 `+08:00`）、
  SQLite；时间值逐微秒正确且不随会话时区漂移。
- **跨语言同库互操作**：同一 PG / MySQL 库、同一份 YAML，Python 执行并写 success 后，
  Go / Java / Rust 均返回 `SKIPPED_ALREADY_DONE`；过期锁可由另一语言接管。
- 8 个示例 YAML 全部通过 `spec/schema.json` 校验。
- 未覆盖：真实 Kubernetes 多副本部署（按需求以并发模拟替代）、MySQL 8 专项验证。

## 已知取舍

1. **过期接管会重复执行**：租约过期后新 Pod 可接管，原 Pod 可能仍在跑。用 `timeout_seconds`
   （≥ p99 耗时 + 余量）和幂等 handler 收敛。
2. **崩溃窗口不做绝对去重**：执行成功与写 success 之间被杀会重跑。
3. **租约依赖 Pod 时钟**：需要 NTP，或把租约设保守。
4. **释放失败会残留锁**：结果仍为 `EXECUTED`，其他 Pod 等到过期。
5. **`version` 是唯一幂等键**：改内容必须 bump version。
6. **HTTP 客户端可注入性不齐**：Java 可注入 `HttpClient`，Go / Python / Rust 用内置客户端，
   代理 / mTLS 场景需要扩展。
7. **没有内建指标与 CI 配置**：只有日志；接入时需在流水线里提供真库。

完整清单与风险分析见 [`docs/design.zh-CN.md`](docs/design.zh-CN.md) §11。

## 许可证

MIT，见 [`LICENSE`](LICENSE)。
