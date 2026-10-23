English: [README.md](README.md)

# startup-task-spring-boot3-starter

[`startup-task-sdk`](../sdk-java) 的 Spring Boot 3 starter：应用启动阶段跑初始化任务，
同一版本只成功执行一次，多 Pod 通过应用数据库协调。

要求 **Java 21** + **Spring Boot 3**。

```xml
<dependency>
  <groupId>io.github.kuun993</groupId>
  <artifactId>startup-task-spring-boot3-starter</artifactId>
  <version>0.1.0</version>
</dependency>
```

## 快速开始

`application.yml`：

```yaml
startup-task:
  enabled: true
  owner-id: ${HOSTNAME:local}       # K8s 下就是 Pod 名
  failure-policy: warn              # warn | fail-fast
  initialize-schema: false          # 生产建议用 Flyway 建表
  specs:
    - classpath:startup/grafana-dashboard.yaml
    - file:/etc/app/startup/*.yaml  # 目录会展开成 *.yaml / *.yml，按文件名排序
```

`src/main/resources/startup/grafana-dashboard.yaml`：

```yaml
task_id: grafana/publish-dashboard
version: "${app.version}"           # 解析前会先用 Spring Environment 解析占位符
timeout_seconds: 60
execution:
  executor: http
  config:
    method: POST
    url: "${grafana.url}/api/dashboards/db"
    headers:
      Authorization: "Bearer ${grafana.token}"
      Content-Type: "application/json"
    body_source: file
    body_path: "dashboard.json"     # spec 来自 file: 位置时，相对路径基于该文件所在目录
    success_check:
      http_status: [200]
```

自定义 handler（不需要 XML，也不需要手动注册）：

```java
@StartupTaskHandler("warm_up_cache")
class WarmUpCacheHandler implements TaskHandler {
    @Override
    public void handle(Map<String, Object> config) {
        // config 就是 YAML 里的 execution.config
    }
}
```

## 配置项

| 配置 | 默认值 | 说明 |
|---|---|---|
| `startup-task.enabled` | `false` | 不打开则 starter 完全不生效 |
| `startup-task.owner-id` | `${HOSTNAME:local}` | 写入锁表的 owner |
| `startup-task.failure-policy` | `warn` | `warn` 只记日志；`fail-fast` 让启动失败 |
| `startup-task.initialize-schema` | `false` | 是否执行 `Schema.ensure`（生产建议用 Flyway） |
| `startup-task.specs` | `[]` | `classpath:` / `file:` 模式或目录 |

## 行为说明

- 任务在 `ApplicationRunner` 中执行，晚于 DataSource / Flyway 初始化。
- spec 里的 Spring 占位符（`${...}`）先从 `Environment` 解析，所以可以来自 Vault、Config Server
  或 profile 配置；解析不到的占位符会原样保留，由 SDK 再从 OS 环境变量展开。
- 方言从连接元数据自动识别：PostgreSQL、MySQL/MariaDB、SQLite。
- `task_id` 重复、spec 读不到、数据库不支持，都会**直接让启动失败**（与 `failure-policy` 无关）：
  这类问题说明没有任何任务真正跑起来，而策略只针对任务结果。
- 暴露 `StartupTaskState` bean；健康检查在任务轮次结束前返回 `OUT_OF_SERVICE`，有任务失败返回 `DOWN`。
  想让 readiness 等它：

```yaml
management:
  endpoint:
    health:
      group:
        readiness:
          include: readinessState,startupTask
```

## 限制

- 每个应用只支持一个 `DataSource`，多数据源不支持。
- 任务是同步执行的，没有异步 / 虚拟线程模式。
- 暂不支持 Spring Boot 4 与 native image。
