Chinese: [README.zh-CN.md](README.zh-CN.md)

# startup-task-spring-boot3-starter

Spring Boot 3 starter for [`startup-task-sdk`](../sdk-java): run initialization tasks at startup,
once per version, coordinated across pods through the application database.

Requires **Java 21** and **Spring Boot 3**.

```xml
<dependency>
  <groupId>io.github.kuun993</groupId>
  <artifactId>startup-task-spring-boot3-starter</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Quick start

`application.yml`:

```yaml
startup-task:
  enabled: true
  owner-id: ${HOSTNAME:local}       # the pod name in Kubernetes
  failure-policy: warn              # warn | fail-fast
  initialize-schema: false          # production: create the tables with Flyway instead
  specs:
    - classpath:startup/grafana-dashboard.yaml
    - file:/etc/app/startup/*.yaml  # directories expand to *.yaml / *.yml, sorted by name
```

`src/main/resources/startup/grafana-dashboard.yaml`:

```yaml
task_id: grafana/publish-dashboard
version: "${app.version}"           # Spring placeholders are resolved before parsing
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
    body_path: "dashboard.json"     # relative to the spec file when it is a file: location
    success_check:
      http_status: [200]
```

Custom handler (no XML, no manual registration):

```java
@StartupTaskHandler("warm_up_cache")
class WarmUpCacheHandler implements TaskHandler {
    @Override
    public void handle(Map<String, Object> config) {
        // config is execution.config from the YAML
    }
}
```

## Configuration

| Property | Default | Meaning |
|---|---|---|
| `startup-task.enabled` | `false` | The starter is inert unless this is true |
| `startup-task.owner-id` | `${HOSTNAME:local}` | Owner written to the lock table |
| `startup-task.failure-policy` | `warn` | `warn`: log and continue; `fail-fast`: abort startup |
| `startup-task.initialize-schema` | `false` | Run `Schema.ensure` (prefer Flyway in production) |
| `startup-task.specs` | `[]` | `classpath:` / `file:` patterns or directories |

## Behaviour

- The task round runs in an `ApplicationRunner` after datasource / Flyway initialization.
- Spring placeholders (`${...}`) in the spec are resolved from the `Environment` first, so values can
  come from Vault, Config Server or profile-specific YAML. Unresolvable placeholders are left
  untouched and then expanded by the SDK from OS environment variables.
- Database dialect is detected from the connection: PostgreSQL, MySQL/MariaDB or SQLite.
- Duplicate `task_id`s, unreadable specs and unsupported databases fail startup regardless of
  `failure-policy` — no task ran, so the policy (which is about task outcomes) does not apply.
- `StartupTaskState` is exposed as a bean; the health indicator reports `OUT_OF_SERVICE` until the
  round finishes and `DOWN` if any task failed. To make readiness wait for it:

```yaml
management:
  endpoint:
    health:
      group:
        readiness:
          include: readinessState,startupTask
```

## Limitations

- One `DataSource` per application; multiple datasources are not supported.
- Tasks run synchronously during startup; there is no async / virtual-thread mode.
- Spring Boot 4 and native image are not supported yet.
