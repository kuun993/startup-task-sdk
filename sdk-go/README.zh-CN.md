English: [README.md](README.md)

# startup-task-sdk / Go


跨语言启动任务协调 SDK 的 Go 实现。行为基准是 [`../docs/design.zh-CN.md`](../docs/design.zh-CN.md)，
本包逐条对齐该契约：多 Pod/多进程启动时，同一个 `(task_id, version)` 只会有一次
`EXECUTED`，其余实例要么 `SKIPPED_ALREADY_DONE`，要么 `SKIPPED_LOCKED`。

```go
import startuptask "github.com/kuun993/startup-task-sdk/sdk-go"
```

## 1. 初始化

三种 Store 构造器对应三套 SQL 方言，`*sql.DB` 由调用方提供（驱动由调用方导入）：

```go
// SQLite（内置/测试/单机）
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

// owner_id 留空 => "<hostname>/<random8>"，进程内只计算一次（sync.Once），
// 保证释放锁能匹配到自己
coordinator := startuptask.NewCoordinator(store, os.Getenv("POD_NAME"))

// 需要可控时钟（测试/回放）时：
coordinator = startuptask.NewCoordinatorWithClock(store, podName, myClock) // myClock 实现 Clock
```

SQLite 用于并发场景时必须 `journal_mode=WAL` + `busy_timeout >= 5000`，否则会看到
`database is locked` 而不是契约里的 `SKIPPED_LOCKED`。

PostgreSQL 的 DSN 注意 `sslmode`：lib/pq 默认 `require`，服务器没开 TLS 时会报
`pq: SSL is not enabled on the server`，此时显式加 `?sslmode=disable`（或按环境要求配置证书）。
MySQL 的 DSN 需要 `parseTime=true` 才能把 `DATETIME(6)` 扫进 `time.Time`。

## 2. 注册 custom handler

```go
err := coordinator.RegisterHandler("warm_up_cache", func(config map[string]any) error {
    keys, _ := config["keys"].([]any)          // YAML 列表解码为 []any
    ttlSeconds, _ := config["ttl_seconds"].(int) // YAML 整数解码为 int
    return warmUpCache(keys, ttlSeconds)
})
```

- handler 签名 `func(config map[string]any) error`：返回 `nil` = 成功，返回 error = 失败，
  panic 也会被捕获并转成 `FAILED`。
- 传入的 `config` 是 `execution.config` 的深拷贝，不含 `handler` 字段；缺省是空 map。
- 重名注册返回 error。

## 3. 运行

```go
spec, err := startuptask.LoadSpec("/etc/tasks/publish_app_settings.yaml") // 文件路径或 YAML 文本都行

result, err := coordinator.Run(ctx, spec) // err 恒为 nil，失败看 result.Outcome / result.Err
log.Printf("%s %s %s %.1fms", result.TaskID, result.Version, result.Outcome, result.DurationMs())

// 串行批量，单个失败不影响后续，返回顺序与入参一致
results := coordinator.RunAll(ctx, []startuptask.TaskSpec{specA, specB})
```

`Run` 不会 panic，也不会把失败抛给调用方：执行阶段的一切失败都收敛为
`Outcome == FAILED` + `Err`。

| Outcome | 含义 |
|---|---|
| `EXECUTED` | 本次抢到执行权并执行成功（已写入 `status='success'`） |
| `SKIPPED_ALREADY_DONE` | `(task_id, version)` 已有 success 记录 |
| `SKIPPED_LOCKED` | 没抢到执行权 |
| `FAILED` | 抢到执行权但执行（或执行前置条件）失败 |

`RunResult` 字段：`TaskID`、`Version`、`Outcome`、`Err`、`Duration`，另有
`DurationMs()` 返回毫秒浮点数（统计口径：从进入 `Run` 到最后一次释放锁尝试结束）。

### 时钟注入

契约 §4.2 要求「当前时间」由 SDK 时钟给出并作为绑定参数传入，因此时钟可替换：

```go
type Clock interface{ Now() time.Time }

startuptask.NewCoordinator(store, owner)                              // SystemClock（默认）
startuptask.NewCoordinatorWithClock(store, owner, myClock)            // 注入时钟
```

注入了时钟后，`acquired_at` / `expires_at` / `executed_at` 和
`RunResult.Duration` 全部由它推导，测试可以精确断言（见
`TestInjectedClockDrivesTimestampsAndDuration`、`TestInjectedClockExpiryBoundary`）。
时钟实现必须是线程安全的（`Run` 可能被多个 goroutine 并发调用）。

## 4. YAML 字段

顶层：

| 字段 | 必填 | 说明 |
|---|---|---|
| `task_id` | 是 | 1..128，匹配 `^[A-Za-z0-9._:/-]+$` |
| `version` | 是 | 1..64，同一 task 下版本变化即视为新批次 |
| `description` | 否 | 说明文本，允许空串 |
| `timeout_seconds` | 否 | 默认 300，必须 > 0 且必须是**整数**（`300.0` 报错）；同时是请求超时与锁的租期，超过 100 年按 100 年计 |
| `execution` | 是 | 见下 |

`execution`：

| 字段 | 说明 |
|---|---|
| `executor` | `http` 或 `custom`（大小写敏感） |
| `handler` | `executor=custom` 时必填，调用方注册的 handler 名 |
| `config` | `http` 时结构固定（见下），`custom` 时原样深拷贝传给 handler |

`execution.config`（`executor=http`）：

| 字段 | 说明 |
|---|---|
| `method` | 必填，交给 HTTP 客户端前统一转大写 |
| `url` | 必填，支持 `${ENV}` |
| `headers` | 选填，string→string，value 支持 `${ENV}` |
| `body_source` | 选填：`file` \| `inline` \| `none`（默认 `none`） |
| `body_path` | `body_source=file` 时必填且不能为空串；相对路径按 spec 来源解析（文件加载→文件所在目录，字符串加载→进程工作目录）。不做环境变量替换 |
| `body` | `body_source=inline` 时的请求体，支持 `${ENV}`，缺省空串（空串合法，会发送空请求体） |
| `success_check.http_status` | int 或 int 列表，默认 `200`（同样只接受整数，`200.0` 报错） |

空串口径与其他语言一致：`description`、inline `body`（以及 `headers: {}`）允许为空；
必填项（`task_id`、`version`、`method`、`url`、`handler`、`body_path`）为空串一律按
`is required` 报错。

环境变量替换在**发起请求前**做（`url`、header value、inline body），`NAME` 匹配
`[A-Za-z_][A-Za-z0-9_]*`；变量缺失 → `FAILED`，`Err` 文案固定为
`environment variable "NAME" is not set`。

失败文案固定：`http status 500 not in [200] (body: ...)`、`http request failed: <原因>`。

校验失败时 `LoadSpec` 返回的错误一定带字段路径，例如
`task.timeout_seconds must be > 0`、`execution.config.method is required`、
`execution.config.body_path is required when body_source is file`。

## 5. 运行测试

```bash
cd sdk-go
go mod tidy
go test ./... -count=1
go test ./... -race -count=1   # 需要 CGO_ENABLED=1 且环境里有 C 编译器
go run ./examples
```

验证环境（Windows，无 gcc）未启用 `-race`，`go test ./... -count=1` 为验证入口；
并发场景测试可用 `go test -run TestConcurrentOnlyOneExecutes -count=5` 反复验证。

测试全部走 SQLite 方言 + 真实 SQL（非 mock），覆盖契约 §10 的 6 个场景：

| 测试 | 场景 |
|---|---|
| `TestFirstRunExecuted` | 1. 空库 → `EXECUTED`，success 记录，锁已释放 |
| `TestSecondRunSkippedAlreadyDone` | 2. 再跑 → `SKIPPED_ALREADY_DONE`，锁表运行前后都为空 |
| `TestConcurrentOnlyOneExecutes` | 3. 8 个 goroutine（各持独立 `*sql.DB`）→ 1 `EXECUTED` + 7 `SKIPPED_LOCKED`，handler 只调用 1 次 |
| `TestExpiredLockTakeover` | 4. 已过期锁 → 新 Pod `EXECUTED`，执行期间锁 owner 已是新 Pod |
| `TestFailureReleasesLockAndNoSuccess` | 5. handler 报错 → `FAILED`、锁释放、记录为 `failed`；同 version 重试 → `EXECUTED`，success 覆盖 failed（仍只有 1 行） |
| `TestReleaseDoesNotDeleteOtherOwnerLock` | 6. A 的释放路径不删 B 名下的锁（直接调 `releaseLock`，另有走完整 `Run` 的版本） |

补充：`TestDoubleCheckSkipsWhenSuccessAppeared`（抢锁后双重检查）、
`TestLockedByOtherOwnerSkips`、`TestHandlerPanicBecomesFailed`、
`TestHandlerReceivesCopiedConfig`、HTTP 执行器（成功/非预期状态码/传输错误/缺失环境变量）、
spec 校验负例（含 `timeout_seconds: 300.0`、`body_path: ""` 等空串/浮点口径）、
方言冲突识别与时间绑定，以及：

| 测试 | 覆盖点 |
|---|---|
| `TestAcquireErrorFailsWithoutRecordsOrRelease` | 抢锁阶段非冲突 SQL 错误 → `FAILED`、不写版本记录、不释放锁 |
| `TestReleaseFailureDoesNotChangeOutcome` | Q5 失败只记日志 → outcome 仍 `EXECUTED`，锁留到租期结束 |
| `TestInjectedClockDrivesTimestampsAndDuration` | 注入时钟决定 `acquired_at` / `expires_at` / `executed_at` / `Duration` |
| `TestInjectedClockExpiryBoundary` | `expires_at == now` 仍算被占用，`now - 1µs` 可接管（严格 `<`） |
| `TestHugeTimeoutIsClampedWhenComputingExpiresAt` | `timeout_seconds` 极端值 → `expires_at` 夹到 100 年，不溢出 |
| `TestDefaultOwnerIDIsProcessStable` | 默认 owner 进程内只算一次；显式 owner 原样使用 |

### 集成测试（真实 PostgreSQL / MySQL）

`integration_test.go` 连真库跑同一套协调场景。**没有 DSN 时自动跳过**，
所以 `go test ./...` 在无库环境仍然全绿。

```bash
# DSN 只放在环境变量里，不要写进命令或日志
export STARTUP_TASK_PG_DSN='postgresql://user:pw@host:5432/startup_task_sdk_selftest'
export STARTUP_TASK_MYSQL_DSN='mysql://user:pw@host:3306/startup_task_sdk_selftest'

go test ./... -count=1                      # 两个方言都跑
go test . -run TestIntegrationPostgres -v    # 只跑 PG
go test . -run 'TestIntegrationMySQL/upsert' -v
```

| 环境变量 | 用途 |
|---|---|
| `STARTUP_TASK_PG_DSN` | `postgresql://user:pw@host:port/db`（未写 `sslmode` 时测试自动补 `sslmode=disable`） |
| `STARTUP_TASK_MYSQL_DSN` | `mysql://user:pw@host:port/db`（测试转换成 go-sql-driver 的 DSN，并强制 `parseTime=true&loc=UTC`） |

测试约束：每个用例用唯一 `task_id` 前缀（`itsdk-<dialect>-<纳秒>`）建自己的行，`t.Cleanup`
按前缀删行；只用 `EnsureSchema` 建表，**不 DROP 库/表**；凭据只经环境变量进入进程，不打印。
子用例：`ensure_schema`、`first_run_executed`、`second_run_skipped_already_done`、
`concurrent_only_one_executes`（8 个独立 `*sql.DB`）、`expired_lock_takeover`、
`failure_releases_lock_and_no_success`、`release_does_not_delete_other_owner_lock`，
外加方言专属：

| 方言 | 专属用例 |
|---|---|
| PostgreSQL | `utc_time_binding`（读回 `acquired_at`/`expires_at` 为同一 UTC 时刻、零偏移）；`timezone_session_keeps_lease_semantics`（`SET TIME ZONE 'Asia/Shanghai'` 下写入/读回/严格 `<` 边界都不偏） |
| MySQL | `duplicate_key_1062`（vendor code 1062 判为「锁被占」而非失败）；`upsert_replaces_failed_with_success`（同 version 先 failed 后 success 只有一行且为 success）；`datetime6_round_trips_as_utc`；`stored_wall_clock_is_utc`（写端 DSN 用 `loc=Asia/Shanghai`，UTC 读端仍看到同一时刻，库里字面量是 UTC）；`lease_boundary`（`expires_at == now` 仍被占用、`now-1µs` 可接管） |

MySQL 的 DSN 建议：必须 `parseTime=true`，否则 `DATETIME(6)` 扫不进 `time.Time`。
`loc` 不影响 SDK 写入（见下），但影响你自己读出的 `time.Time` 的时区，建议 `loc=UTC`。

## 6. 实现要点

协调算法（`coordinator.go`，严格按契约 §5 顺序）：

1. Q1 查 `status='success'` 版本记录，命中即 `SKIPPED_ALREADY_DONE`；
2. 取一次 `now`（UTC），`expires = now + timeout_seconds`（`timeout_seconds` 超过 100 年时按
   100 年计，避免 `time.Duration` 溢出）；
3. Q2 原子 `INSERT` 抢锁；主键冲突则 Q3 条件 `UPDATE`（`expires_at < now`）接管过期锁；
   两步都不做「先 SELECT 再 INSERT」；
4. 抢到锁后再次 Q1（接管场景下原 owner 可能已写完 success）→ 命中返回 `SKIPPED_ALREADY_DONE`，
   锁由 `defer` 释放；
5. 执行 executor（http / custom）→ 成功执行 Q4 写 `success`，返回 `EXECUTED`；
   失败写 `failed` 并返回 `FAILED`；
6. `defer` 中执行 Q5 `DELETE ... WHERE task_id = ? AND owner_id = ?`，失败只记日志，不改 outcome。

其它约束：

- SQL 全部参数化，且**不出现** `now()` / `CURRENT_TIMESTAMP`；时间一律由 SDK 时钟给出 UTC 值后绑定。
  PG 绑定 `time.Time`（UTC），MySQL 绑定 UTC 字面量 `YYYY-MM-DD HH:MM:SS.ffffff`，
  SQLite 绑定同格式 TEXT（字典序即时间序）。
- MySQL 绑字面量而不是 `time.Time` 的原因：go-sql-driver 会把 `time.Time` 换算到 DSN 的
  `loc` 时区再写库，`loc=Local` 时库里就成了本地墙钟，与「DATETIME(6) 存 UTC」的契约冲突，
  会让 Go Pod 与 Python/Java/Rust 差一个时区偏移。字面量绑定让写入与 `expires_at < ?`
  比较都与会话/DSN 时区无关。
- Q4 upsert 分方言：PG/SQLite 用 `ON CONFLICT (task_id, version) DO UPDATE`，MySQL 用
  `ON DUPLICATE KEY UPDATE ... VALUES(...)`。
- 抢锁冲突识别由方言层封装：PG SQLState `23505`；MySQL vendor code `1062`（并接受
  SQLState `23000` + `Duplicate entry`）；SQLite extended code `1555`/`2067`、SQLState `23505`、
  或错误信息含 `UNIQUE constraint failed` / `PRIMARY KEY constraint failed`。
  MySQL 驱动未在依赖里，vendor code 通过 driver error 的 `Number` 字段反射读取，不引入额外依赖。
- 抢锁阶段（Q2/Q3）的非冲突 SQL 错误按契约 §7 处理：`FAILED`，且**不写**版本记录、不释放锁
  （此时无法安全判断锁归属）。
- 日志用 `log/slog`，覆盖 `lock acquired` / `lock takeover` / `skip already done` /
  `skip locked` / `release failed`；`Coordinator.SetLogger` 可替换。
- `default owner_id` 由包级 `sync.Once` 缓存（`<hostname>/<8 位小写 hex>`），进程内不变；
  显式传入 `ownerID` 时原样使用。
- `Clock` 接口（`SystemClock` 默认）是唯一时间源：`acquired_at`、`expires_at`、
  `executed_at` 与 `RunResult.Duration` 都由它推导，因此 `Duration` 在注入时钟下可精确断言。
- 可替换实现：`Store` 是接口，需要特殊后端时按 5 个方法自行实现即可。

## 7. 已知取舍

- **接管会造成重复执行**：`expires_at < now` 时新 owner 可以接管，而原 owner 可能仍在执行，
  因此非幂等操作仍有极小概率被执行两次。这是契约 §5 明确接受的取舍，SDK 只保证
  「同一 `(task_id, version)` 至多一条 `success` 记录」，不保证物理上只执行一次。
- **租期即 `timeout_seconds`**：执行时间接近或超过 `timeout_seconds` 时，其他 Pod 会接管并重复执行；
  请把 `timeout_seconds` 设为执行耗时的上界（HTTP 执行器同时用它做请求超时）。
- **释放失败只记日志**：Q5 失败（例如网络抖动）时 outcome 不变，锁会留到 `expires_at` 之后被接管。
- **`failed` 记录不阻止重试**：只有 `success` 会跳过，失败后下一次启动会重新执行。
- **注入假时钟时 `Duration` 也是假时钟的差值**：`Duration` 的统计口径是
  「进入 `Run` → 最后一次释放锁尝试结束」，两端都读 `Clock`；用 `SystemClock` 时即真实耗时，
  注入测试时钟时得到的是该时钟的差值（这正是指定它能确定性断言的原因）。
- **墙钟粒度决定 `Duration` 精度**：部分平台的时钟量子较粗（Windows 上实测约 1ms），
  亚毫秒级的任务可能测到 `0ms`；`Duration` 只适合看量级，别拿它做性能基准，
  需要精确值请注入 `Clock`。
- **SQLite 仅用于测试/嵌入式**：并发写为库级串行，跨 Pod 协调请用 PostgreSQL / MySQL。

## 8. 示例

`examples/` 下有可直接运行的两个任务（四个语言同名同内容）：

```bash
cd sdk-go
go run ./examples
```

| 文件 | 说明 |
|---|---|
| `examples/publish_app_settings.yaml` | `executor: http`：`PUT ${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings`，`Authorization: Bearer ${CONFIG_CENTER_TOKEN}`，`body_source: file` 读 `app_settings.json`，`success_check.http_status: [200, 201]` |
| `examples/app_settings.json` | 上面任务的请求体（feature flags + limits） |
| `examples/warm_up_cache.yaml` | `executor: custom` + `handler: warm_up_cache`，`config` 带 `keys` 与 `ttl_seconds` |
| `examples/main.go` | 建 SQLite 库、注册 `warm_up_cache` handler、`RunAll` 两个任务并打印结果 |

`go run ./examples` 在未设置环境变量时自动套用示例默认值
`CONFIG_CENTER_URL=http://127.0.0.1:8080`、`CONFIG_CENTER_TOKEN=example-token`、
`APP_NAME=demo-app`；配置中心不可达时该任务输出 `FAILED` 并打印原因（不 panic），
custom 任务正常 `EXECUTED`。

示例默认使用 `%TEMP%/startup-task-sdk-examples.db`（可用 `TASK_DB_PATH` 覆盖）。
第二次运行可以看到 custom 任务变成 `SKIPPED_ALREADY_DONE`（http 任务仍会重试，因为 `failed`
记录不阻止重跑）；删掉该 db 文件即可重来。
