English: [README.md](README.md)

# startup-task-sdk（Java）


跨语言「启动任务协调 SDK」的 Java 实现。Kubernetes 多 Pod 同时启动时，保证同一
`(task_id, version)` 初始化任务只成功执行一次（例如发布应用默认配置、预热缓存）。

行为基准是 `../docs/design.zh-CN.md`（normative），本实现逐条对齐，**无偏差**。
四种语言（python / java / go / rust）的协调算法、SQL 语义、结果枚举完全一致。

## 环境与依赖

- JDK 21（`java -version` 需为 21）
- Maven 3.9+
- 运行时依赖只有 `org.yaml:snakeyaml:2.3`；`junit-jupiter` / `sqlite-jdbc` 均为 test scope。
  HTTP 用 JDK 自带 `java.net.http.HttpClient`，JSON 不需要解析。

## 构建与测试

```bash
cd sdk-java
mvn -B test
```

> 系统未安装 `mvn` 时，可用任意 Maven 3.9+ 发行版（解压后者调用其 `bin/mvn`）；
> 下文命令里的 `mvn` 均指该可执行文件。

默认 `mvn test` 跑 SQLite 方言的真实 SQL 用例（xerial sqlite-jdbc，test scope）：每个线程独立连接，
连接建立时执行 `PRAGMA busy_timeout=10000`，首次连接把库切成 `PRAGMA journal_mode=WAL`。
真实 PostgreSQL / MariaDB 的集成测试需要 DSN 环境变量，未设置时自动 skip（见「集成测试」）。

## 快速开始

```java
import io.startuptask.*;

// 1) 选一种 Store（PostgreSQL / MySQL / SQLite）
Store store = new PostgresStore(dataSource);                 // 或 new PostgresStore(url, user, password)
// Store store = new MysqlStore(dataSource);
// Store store = new SqliteStore("jdbc:sqlite:/tmp/startup-task.db");

// 2) 建表（幂等，可每次启动都调用）
Schema.ensure(store);

// 3) 创建协调器；ownerId 建议用 Pod 名，传 null 时默认 "<hostname>/<uuid8>"（进程内固定）
Coordinator coordinator = new Coordinator(store, System.getenv("POD_NAME"));

// 4) 注册 custom handler（重名注册抛 IllegalStateException）
coordinator.registerHandler("warm_up_cache", config -> {
    List<String> keys = (List<String>) config.get("keys");
    int ttlSeconds = (int) config.getOrDefault("ttl_seconds", 300);
    // ... 业务逻辑
});

// 5) 解析 YAML 并执行
TaskSpec spec = TaskSpecLoader.load("/etc/startup/publish_app_settings.yaml"); // 也接受 YAML 文本
RunResult result = coordinator.run(spec);
if (result.failed()) {
    throw new IllegalStateException("startup task failed: " + result.error());
}

// 批量：串行执行，单个失败不影响后续，返回值与入参同序
List<RunResult> results = coordinator.runAll(List.of(specA, specB));
```

`RunResult` 是不可变对象，字段 `taskId / version / outcome / error / durationMs`，
`outcome` 取 `EXECUTED | SKIPPED_ALREADY_DONE | SKIPPED_LOCKED | FAILED`，
`toString()` 形如
`RunResult[taskId=…, version=…, outcome=EXECUTED, error=null, durationMs=35]`。

`run()` 不抛受检异常：执行阶段的一切失败都收敛为 `FAILED` 并写入 `status='failed'` 的版本记录。

## YAML 任务定义

```yaml
task_id: config/publish-app-settings # 必填，1..128，匹配 [A-Za-z0-9._:/-]+
version: "2026-10-22-v1"             # 必填，1..64；同 task_id 下版本变化即新批次
description: 发布应用默认配置到配置中心  # 选填
timeout_seconds: 120                 # 选填，默认 300，必须 > 0；同时是僵尸锁超时（锁窗口上限 100 年）
execution:                           # 必填
  executor: http                     # 必填，http | custom（大小写敏感）
```

### executor: http

```yaml
execution:
  executor: http
  config:
    method: PUT                      # 必填，大写后交给 HttpClient
    url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"   # 必填，支持 ${ENV}
    headers:                         # 选填，string->string，value 支持 ${ENV}
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"
      Content-Type: application/json
    body_source: file                # 选填：file | inline | none（默认 none）
    body_path: app_settings.json     # body_source=file 时必填
    body: '{"a":1}'                  # body_source=inline 时使用（选填，缺省空串），支持 ${ENV}
    success_check:
      http_status: [200, 201]        # 选填，int 或 int 列表，默认 200
```

- `${NAME}`（`[A-Za-z_][A-Za-z0-9_]*`）在**发起 HTTP 前**替换：`url`、`headers` 的值、inline `body`。
  文件 body 与 `body_path` 不替换。
- 变量缺失 → `FAILED`，`error = 'environment variable "NAME" is not set'`。
- 状态码不在白名单 → `FAILED`，`error = 'http status 500 not in [200] (body: …)'`；
  传输错误 → `'http request failed: <原因>'`。
- 请求超时 = `timeout_seconds` 秒。
- `body_path` 为相对路径时：spec 由文件加载 → 相对该文件所在目录；由字符串加载 → 相对进程工作目录。
- body 文件按**字节**原样发送（`Files.readAllBytes` + `ofByteArray`），非 UTF-8 载荷不会被破坏；
  相对路径不存在的文件 → `FAILED`，`error = 'body file "…" cannot be read: …'`。
- `description` 与 inline `body` 允许空串；`body_source: file` 时 `body_path` 为空串仍按缺失报错。

### executor: custom

```yaml
execution:
  executor: custom
  handler: warm_up_cache                     # 必填，非空，需在 Coordinator 上注册
  config: { keys: ["home:feed"], ttl_seconds: 300 }   # 选填，深拷贝后原样传给 handler；缺省空 map
```

handler 未注册 → `FAILED`，`error = 'handler "xxx" is not registered'`。
handler 抛异常 → `FAILED`（`error` 取异常 message）。

### 校验

`TaskSpecLoader.load(...)` 校验失败抛 `SpecException`（`RuntimeException`），
错误信息带字段路径，例如 `task.timeout_seconds must be > 0`、
`execution.config.method is required`、`task.task_id must match ^[A-Za-z0-9._:/-]+$`。
未知字段同样报错（`task.foo is not allowed`）。

## 示例

`examples/` 下三个文件：

| 文件 | 说明 |
|---|---|
| `publish_app_settings.yaml` | `executor: http`，`PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings`，header 用 `${CONFIG_CENTER_TOKEN}`，body 来自文件 |
| `app_settings.json` | 上述任务的 body 文件（应用默认配置） |
| `warm_up_cache.yaml` | `executor: custom`，`handler: warm_up_cache`，`config.keys` + `config.ttl_seconds` |

`io.startuptask.examples.ExampleRunner` 是会真正跑起来的主类：建 SQLite 库、建表、
注册 `warm_up_cache` handler、`runAll` 一次、再 `runAll` 一次
（第二次成功的任务 `SKIPPED_ALREADY_DONE`，失败的任务按契约重试）。

示例环境变量（可用同名 `-D` 或环境变量覆盖，未设置时用默认值）：

| 变量 | 默认值 |
|---|---|
| `CONFIG_CENTER_URL` | `http://127.0.0.1:8080` |
| `CONFIG_CENTER_TOKEN` | `example-token` |
| `APP_NAME` | `demo-app` |

默认地址通常没有配置中心在监听，所以 http 示例会以 `FAILED` 收尾（`http request failed: …`
或 `http status 404 not in [200, 201] …`），**不会抛异常**；custom 示例照常 `EXECUTED`。
想看到成功路径，把 `CONFIG_CENTER_URL` 指向一个真实配置中心即可：

```bash
java -DCONFIG_CENTER_URL=http://127.0.0.1:8080 -cp "target/classes;$(cat target/cp.txt)" \
  io.startuptask.examples.ExampleRunner
```

**可跑通的命令**（Git Bash，工作目录 `sdk-java`）：

```bash
# 生成运行时 classpath（sqlite-jdbc 是 test scope，示例用 includeScope=test 一并带上）
mvn -q -B test-compile \
  dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=target/cp.txt

java -cp "target/classes;$(cat target/cp.txt)" io.startuptask.examples.ExampleRunner
```

输出类似（时间戳/耗时/owner 每次不同）：

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

删除 `target/examples/startup-task.db`（或传第二个参数指定状态目录）即可重跑第一次的效果。
不接 `java -cp` 也可以直接跑示例测试：

```bash
mvn -q -B test -Dtest=ExamplesTest
```

## 协调算法

严格按 `design.zh-CN.md` §5 的顺序：

1. **查版本**：`SELECT 1 … status='success'` 命中 → `SKIPPED_ALREADY_DONE`（`failed` 记录不阻止重试）。
2. **抢执行权**（两条原子路径，绝不 `SELECT` 后 `INSERT`）：
   - `INSERT INTO task_execution_lock …`，主键冲突视为「已被占用」；
   - 冲突则执行 `UPDATE … SET owner_id=?, acquired_at=?, expires_at=? WHERE task_id=? AND expires_at < ?`，
     影响 1 行才算拿到锁（过期接管）。
   两条都没拿到 → `SKIPPED_LOCKED`。
3. **双重检查**：拿到锁后再查一次版本记录，命中 → `SKIPPED_ALREADY_DONE`（接管场景原 owner 可能已写完 success），
   随后仍会释放自己的锁。
4. **执行** executor（http 或 custom），成功写 `status='success'` → `EXECUTED`；失败写 `status='failed'` → `FAILED`。
5. **释放**：`DELETE FROM task_execution_lock WHERE task_id=? AND owner_id=?`，
   在 `finally` 中执行（含步骤 3 的提前返回），只删自己名下的锁；释放失败只记日志、不改变 outcome。

其他约定：

- 所有 SQL 用 `PreparedStatement` 绑定参数，**不出现** `now()` / `CURRENT_TIMESTAMP`，
  「当前时间」由 `Clock` 计算；`acquired_at` 与 `expires_at` 用同一个 `now`。
- 抢锁阶段遇到**非主键冲突**的异常（连接错误等）直接 `FAILED`，不写版本记录、不释放锁。
- 执行阶段（步骤 4 与 4a）捕获 `Throwable`：普通 `Error`（如 handler 抛 `AssertionError`）与异常一样
  收敛为 `FAILED`（写 `failed` 记录 + finally 释放锁）；`VirtualMachineError`（OOM / StackOverflowError）
  这类致命错误只记日志后重抛，不写版本记录，但 finally 仍会释放本 Pod 的锁。
- `expires_at` 的锁窗口夹到 100 年上限（与 Python/Rust 对齐），不影响 `> 0` 的校验口径。
- 每次 store 调用独立开连接、只做一件事：连接为 `autoCommit=false` 时由 SDK 自行提交成功的工作、
  回滚失败的工作（PG 上主键冲突会让事务进入 aborted，必须先回滚再执行 Q3 过期接管）。
- 时间是 UTC。SQLite 存文本 `YYYY-MM-DD HH:MM:SS.ffffff`（6 位小数，字典序即时间序）；
  PG 绑定 `Timestamp.from(Instant)` 并显式传 UTC `Calendar`；MySQL 绑定同样格式的 UTC 文本
  （`mysql-connector-j` 向 MariaDB 10.4 的 `DATETIME(6)` 调 `setTimestamp` 会丢小数秒，见「集成测试」）。
- 主键冲突识别封装在 `Dialect`：PG `23505`、MySQL `1062`、SQLite `1555`/`2067`/`23505`/
  错误信息含 `UNIQUE constraint failed` 或 `PRIMARY KEY`。不使用 `INSERT IGNORE`。
- `Clock` 可注入（`new Coordinator(store, ownerId, clock)`），便于测试。

## 日志

用 `java.util.logging`（无 SLF4J 依赖）。日志覆盖 `lock acquired` / `lock takeover` /
`skip already done` / `skip locked` / `release failed`。默认走容器/父 logger 配置；
也可以用 `Logs.configure(Level.INFO)` 装一个简洁的单行 handler。

> 注意：xerial sqlite-jdbc 在建立第一条连接时会重置 JUL 配置（handler 与 level 被清空）。
> 用 SQLite 时请把 `Logs.configure(...)` 放在第一次连库之后（`ExampleRunner` 就是这么做的），
> 生产环境用 PG/MySQL 不受影响。

## 集成测试（真实 PostgreSQL / MySQL）

`IntegrationPgTest` / `IntegrationMysqlTest` 连真库跑同一套场景。DSN 从环境变量读取，
**只在进程内存中使用**：解析失败的异常信息、`toString()` 都不会回显 DSN 或密码。

| 环境变量 | 形式 | 驱动 |
|---|---|---|
| `STARTUP_TASK_PG_DSN` | `postgresql://user:password@host:5432/db` | `org.postgresql:postgresql`（42.7.x, test scope） |
| `STARTUP_TASK_MYSQL_DSN` | `mysql://user:password@host:3306/db` | `com.mysql:mysql-connector-j`（9.x, test scope） |

```bash
cd sdk-java

# 无 DSN：集成测试自动 skip，其余用例照常跑
mvn -B -o test

# 有 DSN（用你自己的环境变量，不要 echo 凭据）
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'
mvn -B test
```

要求：数据库已存在、账号有建表/增删行权限。测试只做 `CREATE TABLE IF NOT EXISTS`（`Schema.ensure`）
和自带 `it-<TestClass>-<uuid8>-` 前缀的行，跑完按前缀清理，**不会 DROP DATABASE**。
DSN 缺失时 `Assumptions.assumeTrue` 让整个类 skip，所以无库环境 `mvn test` 依然全绿。

每个方言都覆盖：

- `Schema.ensure` 真执行：PG 断言 `timestamp without time zone` / `character varying(128)`；
  MySQL 断言 `datetime` + `datetime_precision=6`。
- 首次 `EXECUTED`（success 记录、锁表清空）；同 version 再跑 `SKIPPED_ALREADY_DONE`（锁表始终为空）。
- 8 线程 / 8 个独立 Coordinator / 8 个 owner 并发 → 恰好 1 个 `EXECUTED`、7 个 `SKIPPED_LOCKED`、handler 1 次。
- 直接插入过期锁 → 新 Pod `EXECUTED`，执行期间锁 owner 已变为新 Pod。
- handler 抛异常 → `FAILED`、锁释放、记录 `failed`。
- A 抢锁后被改成 B，A 调 `releaseLock` → B 的锁仍在。

方言专属：

- **PG 时区**：`TimeZone.setDefault(Asia/Shanghai)` 后用已知 UTC 时刻抢锁/写版本记录，
  再用 `getTimestamp(col, utcCalendar)` 读回，断言与下发的 UTC 时刻逐微秒一致。
- **PG 非 autocommit（25P02 回归）**：包装 `SimpleDataSource`，让它返回 `autoCommit=false` 的连接。
  Pod B 先用它抢一个未过期的锁（应 `SKIPPED_LOCKED`，不能因 aborted 事务变成 `FAILED`），
  再把锁改成已过期，Pod B 必须能接管成功（`EXECUTED`）。
- **MySQL**：`ON DUPLICATE KEY UPDATE` 同 version 只留一行；`1062` 被识别为「锁被占用」而不是异常；
  `DATETIME(6)` 在 `Asia/Shanghai` 默认时区下仍按 UTC 逐微秒回读。

## 已知取舍

- **接管会带来重复执行**：`expires_at < now` 的锁会被其他 Pod 接管，而原 owner 可能仍在执行。
  按契约 §5 要点 3，这是规范接受的取舍；SDK 不为崩溃接管下的非幂等操作提供绝对去重保证。
- `timeout_seconds` 同时充当执行超时与僵尸锁判定，取值要大于任务实际最长耗时，否则会提前被接管。
- 步骤 3 双重检查只是缩小窗口，不是分布式事务：两次检查之间仍可能有并发写入（无实际影响，
  因为写的是同一条 `(task_id, version)` 记录）。
- 抢锁阶段的非冲突异常（连接失败）按契约不写版本记录、不释放锁；这类情况需要人工/重启恢复。
- 若 DataSource 返回的连接是 `autoCommit=false`，SDK 会按「每次调用一个工作单元」自行提交/回滚；
  若调用方想用同一个连接包住更大的事务，请不要把这种连接交给 SDK。
- 集成测试需要真库 DSN（未配置时自动 skip）。验证环境为 PostgreSQL 14.6 与 MariaDB 10.4.24：
  PG 的时区绑定、非 autocommit 接管（25P02）均已实测通过；MySQL 方言采用 UTC 文本绑定
  （`SqlTypes`），因为 `mysql-connector-j` 的 `setTimestamp` 在 MariaDB 上会丢小数秒
  （8.4.0 / 9.2.0 均可复现）。

## 目录

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
    ├── CoordinatorContractTest.java   # 契约 §10 六个场景 + 双重检查 + 步骤 1 不触碰锁表
    ├── CoordinatorMiscTest.java       # 重名注册 / 未注册 handler / 重试 / runAll 顺序 / 抢锁异常
    ├── StoreTransactionTest.java      # autoCommit=false 连接：提交 / 回滚 / 过期接管
    ├── TaskSpecLoaderTest.java        # 解析正例与校验负例
    ├── HttpExecutorTest.java          # 200/201/500、${ENV} 替换、文件 body、传输错误
    ├── ExamplesTest.java              # examples/ 解析与端到端执行
    ├── DialectTest.java               # 方言 SQL / 冲突识别 / 时间格式
    ├── IntegrationPgTest.java         # 真库 PG：时区 / 非 autocommit 25P02 / 通用场景
    ├── IntegrationMysqlTest.java      # 真库 MySQL：upsert / 1062 / DATETIME(6) / 通用场景
    ├── AbstractStoreIntegrationTest.java  # 两个方言共用的集成场景与 SQL helper
    ├── JdbcDsn.java                   # DSN 解析（凭据不外泄）
    └── SqliteFixture.java / DelegatingStore.java
```
