English: [README.md](README.md)

# startup-task-sdk (Python)

跨语言启动任务协调 SDK 的 Python 实现。行为基准是仓库根的
[`docs/design.zh-CN.md`](../docs/design.zh-CN.md)，YAML 结构以 [`spec/schema.json`](../spec/schema.json)
为准，建表语句见 [`spec/ddl/*.sql`](../spec/ddl)。

同一 `(task_id, version)` 在多 Pod 同时启动时只会成功执行一次：SDK 负责
「查版本 → 抢执行权 → 执行 → 记录结果 → 释放锁」，执行动作由 YAML 声明
（内置 `http` 执行器，或调用方注册的 `custom` handler）。

## 安装

```bash
cd sdk-python
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e .          # Windows
# ./.venv/bin/python -m pip install -e .                # Linux / macOS
```

运行依赖：`PyYAML`、`requests`。数据库驱动是可选 extras：
`pip install "startup-task-sdk[postgres]"`（psycopg2-binary）、
`[mysql]`（PyMySQL）或 `[all]`。测试需要 `pytest`
（`pip install -e ".[dev]"` 或 `pip install -r requirements-dev.txt`）。

要求 Python >= 3.10。

## 快速开始

```python
from startup_task import Coordinator, SqliteStore, ensure_schema, load_spec

store = SqliteStore("startup_tasks.db")   # 见下方「三种 Store」
ensure_schema(store)                      # 建表（幂等）

coordinator = Coordinator(store)          # owner_id 默认 <hostname>/<uuid8>
results = coordinator.run_all([
    load_spec("examples/publish_app_settings.yaml"),
    load_spec("examples/warm_up_cache.yaml"),
])
for result in results:
    print(result.task_id, result.version, result.outcome, result.error, result.duration_ms)
```

`load_spec` 同时接受 YAML 文本和文件路径；校验失败抛 `SpecError`（`ValueError`
子类），错误信息里带字段路径，例如 `task.timeout_seconds must be > 0`、
`execution.config.method is required`。

### 三种 Store

```python
from startup_task import PostgresStore, MysqlStore, SqliteStore

# SQLite：测试 / 嵌入式；时间以 TEXT 'YYYY-MM-DD HH:MM:SS.ffffff'（UTC）存储
store = SqliteStore("startup_tasks.db", busy_timeout_ms=10000)

# PostgreSQL：时间以原生 TIMESTAMP（UTC）绑定；强制配置建议加 statement_timeout
store = PostgresStore("postgresql://user:pass@host:5432/db?options=-c%20statement_timeout%3D5000")

# MySQL / MariaDB：时间以原生 DATETIME(6)（UTC）绑定
store = MysqlStore("mysql://user:pass@host:3306/db?charset=utf8mb4")
```

三种 Store 都实现了同一个 `Store` 协议（Q1–Q5 五条参数化 SQL），方言差异只体现在
主键冲突识别和时间绑定上。也可以传入自定义 `connect` 工厂来复用连接池：

```python
store = PostgresStore(connect=pool.getconn)   # 返回的 psycopg2 连接需可 autocommit
```

`owner_id` 请在生产环境显式传入 Pod 名（`Coordinator(store, owner_id=os.environ["HOSTNAME"])`），
默认值在进程内固定不变，满足释放锁时匹配自己的要求。

### 注册 custom handler

```python
def warm_up_cache(config: dict) -> None:
    keys = config.get("keys", [])
    ttl_seconds = config.get("ttl_seconds", 600)
    ...  # 正常返回 = 成功；抛异常 = FAILED，异常消息进 RunResult.error

coordinator.register_handler("warm_up_cache", warm_up_cache)
```

handler 签名是 `Callable[[dict], None]`，收到的对象是 `execution.config` 的**深拷贝**
（不含 `handler` 字段），改它不会影响 spec。重复注册同名 handler 抛 `ValueError`；
handler 未注册时 `run` 返回 `FAILED`，`error = 'handler "xxx" is not registered'`。

## YAML 字段说明

```yaml
task_id: config/publish-app-settings  # 必填，1..128，字符集 [A-Za-z0-9._:/-]
version: "2026-10-22-v1"              # 必填，1..64；版本变化 = 新批次
description: 说明                      # 选填
timeout_seconds: 60                   # 选填，整数 > 0，默认 300；同时是僵尸锁判定窗口
execution:                            # 必填
  executor: http                      # http | custom（大小写敏感）
```

`executor: http` 时：

| 字段 | 必填 | 说明 |
|---|---|---|
| `config.method` | 是 | HTTP 方法，解析时统一大写 |
| `config.url` | 是 | 目标地址，支持 `${ENV}` |
| `config.headers` | 否 | string → string，值支持 `${ENV}` |
| `config.body_source` | 否 | `file` \| `inline` \| `none`，默认 `none` |
| `config.body_path` | `file` 时必填 | 相对路径：spec 由文件加载则相对该文件目录，由字符串加载则相对进程工作目录 |
| `config.body` | 否 | `inline` 时的请求体，支持 `${ENV}`，缺省空串 |
| `config.success_check.http_status` | 否 | 整数或非空整数列表，默认 `[200]` |

请求超时 = `timeout_seconds` 秒。失败信息格式固定：
`http status 500 not in [200] (body: ...)` / `http request failed: <原因>`。

`executor: custom` 时：

| 字段 | 必填 | 说明 |
|---|---|---|
| `execution.handler` | 是 | 非空字符串，必须是已注册的 handler 名 |
| `execution.config` | 否 | 任意 map，原样（深拷贝）传给 handler，缺省 `{}` |

环境变量替换只作用于 `url`、`headers` 的值和 inline `body`（不替换 `body_path`），
在 HTTP 请求发出前执行；变量缺失 → `FAILED`，`error = 'environment variable "NAME" is not set'`。

## Outcome 语义

| Outcome | 含义 |
|---|---|
| `EXECUTED` | 本次拿到执行权且执行成功，已写入 `status='success'` |
| `SKIPPED_ALREADY_DONE` | `(task_id, version)` 已有 `success` 记录（步骤 1 或步骤 3 双重检查命中） |
| `SKIPPED_LOCKED` | 未拿到执行权（锁被别人持有且未过期） |
| `FAILED` | 拿到执行权但执行失败，或执行前置条件失败（含环境变量缺失、HTTP 非预期状态码、handler 抛错） |

`RunResult` 字段：`task_id` / `version` / `outcome` / `error` / `duration_ms`；
`str(outcome)` 是大写常量名。`failed` 记录不阻止重试，下个 Pod 会重新执行。

## 运行示例

```bash
./.venv/Scripts/python.exe examples/run_examples.py
```

`examples/` 下是两个通用场景（四种语言同名同内容）：

| 文件 | 说明 |
|---|---|
| `publish_app_settings.yaml` | `executor: http`，`PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings`，body 来自 `app_settings.json` |
| `app_settings.json` | 上面任务的请求体（feature flags / limits） |
| `warm_up_cache.yaml` | `executor: custom`，`handler: warm_up_cache`，config 带 `keys` 与 `ttl_seconds` |
| `run_examples.py` | 可运行入口：SQLite 建库、注册 `warm_up_cache`、`run_all` 两个任务并打印 `RunResult` |

`run_examples.py` 会给 `CONFIG_CENTER_URL`（默认 `http://127.0.0.1:8080`）、
`CONFIG_CENTER_TOKEN`（默认 `example-token`）、`APP_NAME`（默认 `demo-app`）设置默认值；
再跑一遍 `run_all` 可以看到 `SKIPPED_ALREADY_DONE`（版本号变化的第三次执行仍是 `EXECUTED`）。
没有可用的配置中心时（连接失败或返回非 2xx），http 任务会 `FAILED` 并把原因打印出来，
脚本不崩、退出码 0。

## 运行测试

```bash
./.venv/Scripts/python.exe -m pytest -q
```

测试用真实 SQLite 文件库（WAL + `busy_timeout=10000` + 独立连接），不 mock SQL，
覆盖 `docs/design.zh-CN.md` §10 的 6 个场景、spec 校验负例、http 执行器（本地 HTTP server）、
以及 Outcome 字符串契约。真库集成测试见下一节，未配置 DSN 时会自动 skip。

## 集成测试（真实 PostgreSQL / MySQL）

`tests/test_integration_common.py`、`tests/test_integration_pg.py`、
`tests/test_integration_mysql.py` 会连真实数据库，用 `STARTUP_TASK_PG_DSN` /
`STARTUP_TASK_MYSQL_DSN` 两个环境变量取连接串（**只从环境变量读，不落盘、不打印**）。
未设置对应变量的用例 skip，因此没有数据库时 `pytest -q` 依然全绿。

```bash
# 自行提供连接串（不要写进仓库）
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
./.venv/Scripts/python.exe -m pytest -q

# 只跑集成测试
./.venv/Scripts/python.exe -m pytest -q tests/test_integration_common.py \
    tests/test_integration_pg.py tests/test_integration_mysql.py
```

```powershell
# PowerShell
$env:STARTUP_TASK_PG_DSN = "postgresql://user:password@host:5432/startup_task_sdk_selftest"
$env:STARTUP_TASK_MYSQL_DSN = "mysql://user:password@host:3306/startup_task_sdk_selftest"
```

DSN 格式：`postgresql://user:password@host:port/db`（交给 psycopg2/libpq）、
`mysql://user:password@host:port/db?charset=utf8mb4`（由 `parse_mysql_dsn` 拆成 PyMySQL
的关键字参数）。数据库只需事先建好空库，建表由 `ensure_schema(store)` 完成；测试
**不执行 `DROP DATABASE`**，每个用例用唯一的 `it/<dialect>-<case>-<random>` task_id，
结束时只删除自己写的那几行。

覆盖范围：

- 两种方言共同（`test_integration_common.py`，参数化跑 pg / mysql）：
  `ensure_schema` 在真库上建表并校验列类型（PG `VARCHAR/TIMESTAMP`、
  MySQL/MariaDB `VARCHAR(128)`/`DATETIME(6)`）；首次执行 `EXECUTED` + `success` 记录 +
  锁表清空；同版本再跑 `SKIPPED_ALREADY_DONE` 且锁表前后都为空；8 个独立连接/8 个 owner
  并发只有 1 个 `EXECUTED`、7 个 `SKIPPED_LOCKED`、handler 只调 1 次；过期锁被接管
  （执行中锁 owner 已是新 Pod）；handler 抛错 → `FAILED`、锁释放、记录 `failed`；
  A 的释放路径不会删掉已改到 B 名下的锁；版本号变化视为新批次。
- PostgreSQL 专属（`test_integration_pg.py`）：非 autocommit 连接（`connect=` 注入
  `autocommit=False`）下 Q2 主键冲突后 Q3 接管的两种结局（未过期 → `SKIPPED_LOCKED`，
  已过期 → `EXECUTED`），断言不会出现 `25P02`、不会误判 `FAILED`；时间绑定断言
  `acquired_at/expires_at` 与下发的 UTC 值完全一致，并额外把会话时区设成
  `America/New_York`（默认会话是 `PRC`）验证与 session tz 无关、跨时区会话仍能正确判断过期。
- MySQL / MariaDB 专属（`test_integration_mysql.py`）：`ON DUPLICATE KEY UPDATE` 真的
  upsert（同版本先 `failed` 后 `success`，只剩一行且为 `success`）；真实 `1062` 被识别为
  锁冲突（`DuplicateKeyError` / `SKIPPED_LOCKED`，不是 `FAILED`）；`DATETIME(6)` 读回为
  `datetime`、微秒保留、值为 UTC 墙钟。

## 已知取舍：接管会导致重复执行

持有锁的 Pod 崩溃后，锁不会自动删除；到期（`expires_at < now`）后新 Pod 会通过条件
`UPDATE` 接管并执行任务。此时**原 owner 可能仍在执行**，于是同一个 `(task_id, version)`
可能被执行两次——这是契约明确接受的取舍（§5 要点 3、§11），SDK 不为崩溃接管下的
非幂等操作提供绝对去重保证。

工程上的缓解手段：

- 把任务本身做成幂等（`PUT` 而不是 `POST`，或先查后写）。
- `timeout_seconds` 设为任务最大耗时的上界；太短会放大误接管概率，太长会拖慢故障恢复。
- 适当设置数据库侧 statement timeout / 连接超时，避免长时间挂起被误判为「僵尸」。
- 不要依赖锁表做业务状态，业务状态写在 `task_version_record` 之外。

其它实现说明：

- 所有时间由 SDK 时钟计算并以绑定参数传入，SQL 中不出现 `now()` / `CURRENT_TIMESTAMP`，
  因此可以注入 `clock` 做确定性测试。
- DDL 内嵌在 `startup_task/schema.py`（内容与 `spec/ddl/*.sql` 逐句一致，有测试比对），
  避免安装后依赖仓库目录结构。
- 抢锁只有两条原子路径：Q2 `INSERT` + 方言主键冲突识别，或 Q3 条件 `UPDATE` 接管，
  不存在「先 SELECT 再 INSERT」。
- MySQL 默认返回「变更行数」而不是「匹配行数」，同一时钟、同一 owner 重复接管时 Q3 会
  报 0 行；此时 SDK 回读一次锁行（`owner_id` 与 `expires_at` 都等于本次写入值才算抢到），
  PG/SQLite 仍以 `rowcount == 1` 判定。
- 每条 SQL 语句是独立的执行单元：调用方若注入 `autocommit=False` 的连接（psycopg2 默认），
  SDK 会在语句成功后 commit、失败后 rollback，避免 PG 因 Q2 主键冲突进入 aborted 状态
  （25P02）让随后的 Q3 失败，也不会留下未结束的事务。
- 计算 `expires_at` 时 `timeout_seconds` 夹到 100 年上限（与 Rust 实现对齐），
  避免极端值让 `timedelta` 溢出；校验规则仍是 `> 0`。
- `description` 与 inline `body` 允许空串；`body_source: file` 时 `body_path` 空串视为缺失报错。
- 时间绑定一律是 **UTC 墙钟**：SQLite 用 `TEXT 'YYYY-MM-DD HH:MM:SS.ffffff'`，PostgreSQL /
  MySQL 传 **naive** `datetime`。PG 的列是 `TIMESTAMP`（无时区），psycopg2 会把 tz-aware 值
  按**会话时区**渲染（例如会话是 `PRC` 时 UTC 12:00 会被写成 20:00），所以必须传 naive UTC；
  这样不同会话时区的 Pod 也能对同一批时间做正确比较。
- `MysqlStore` 的 `mysql://` DSN 由 `parse_mysql_dsn()` 拆成 PyMySQL 关键字参数
  （PyMySQL 没有 libpq 风格的 DSN），query 参数（如 `charset`）原样透传。
