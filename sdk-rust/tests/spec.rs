//! Negative validation cases and normal parsing cases for `load_spec` (contract §6).

use startup_task_sdk::{
    load_spec, substitute_env, BodySource, Execution, SpecErrorKind, TaskSpec,
};

fn expect_error(yaml: &str) -> String {
    match load_spec(yaml) {
        Ok(spec) => panic!("expected error, got valid spec: {:?}", spec.task_id),
        Err(err) => err.to_string(),
    }
}

#[test]
fn minimal_http_spec_parses_with_defaults() {
    let spec = load_spec(
        "task_id: config/publish-app-settings\n\
         version: \"1\"\n\
         execution:\n\
         \x20 executor: http\n\
         \x20 config:\n\
         \x20   method: POST\n\
         \x20   url: http://localhost:8080/api/v1/apps/demo-app/settings\n",
    )
    .expect("spec should be valid");

    assert_eq!(spec.task_id, "config/publish-app-settings");
    assert_eq!(spec.version, "1");
    assert_eq!(spec.timeout_seconds, 300, "timeout_seconds defaults to 300");
    assert!(spec.description.is_none());
    assert!(spec.base_dir().is_none());

    match &spec.execution {
        Execution::Http(cfg) => {
            assert_eq!(cfg.method, "POST");
            assert_eq!(cfg.success_status, vec![200]);
            assert_eq!(cfg.body_source, BodySource::None);
            assert!(cfg.headers.is_empty());
        }
        other => panic!("expected http execution, got {:?}", other.executor()),
    }
}

#[test]
fn http_success_status_accepts_int_and_list() {
    let single = load_spec(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: GET\n    url: http://x/\n    success_check:\n      http_status: 204\n",
    )
    .expect("valid");
    match single.execution {
        Execution::Http(cfg) => assert_eq!(cfg.success_status, vec![204]),
        _ => panic!("expected http"),
    }

    let many = load_spec(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: GET\n    url: http://x/\n    success_check:\n      http_status: [200, 201, 202]\n",
    )
    .expect("valid");
    match many.execution {
        Execution::Http(cfg) => assert_eq!(cfg.success_status, vec![200, 201, 202]),
        _ => panic!("expected http"),
    }
}

#[test]
fn custom_config_is_passed_through() {
    let spec = load_spec(
        "task_id: cache/warm-up\n\
         version: \"1\"\n\
         execution:\n\
         \x20 executor: custom\n\
         \x20 handler: warm_up_cache\n\
         \x20 config:\n\
         \x20   keys: [\"home:feed\", \"config:feature-flags\"]\n\
         \x20   ttl_seconds: 300\n",
    )
    .expect("valid");

    match &spec.execution {
        Execution::Custom(cfg) => {
            assert_eq!(cfg.handler, "warm_up_cache");
            assert_eq!(cfg.config["ttl_seconds"], 300);
            assert_eq!(cfg.config["keys"][0], "home:feed");
            assert_eq!(cfg.config["keys"][1], "config:feature-flags");
            assert!(cfg.config.get("handler").is_none(), "handler must not leak into config");
        }
        _ => panic!("expected custom"),
    }
}

#[test]
fn custom_without_config_gets_empty_map() {
    let spec = load_spec(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: custom\n  handler: h\n",
    )
    .expect("valid");
    match &spec.execution {
        Execution::Custom(cfg) => assert_eq!(cfg.config, serde_json::json!({})),
        _ => panic!("expected custom"),
    }
}

#[test]
fn missing_task_id_is_reported_with_field_path() {
    let message = expect_error("version: \"1\"\nexecution:\n  executor: custom\n  handler: h\n");
    assert!(
        message.contains("task.task_id is required"),
        "message = {}",
        message
    );
}

#[test]
fn empty_task_id_is_required_error() {
    let message =
        expect_error("task_id: \"\"\nversion: \"1\"\nexecution:\n  executor: custom\n  handler: h\n");
    assert!(message.contains("task.task_id is required"), "message = {}", message);
}

#[test]
fn task_id_pattern_is_enforced() {
    let message = expect_error(
        "task_id: \"bad id!\"\nversion: \"1\"\nexecution:\n  executor: custom\n  handler: h\n",
    );
    assert!(message.contains("task.task_id must match"), "message = {}", message);
}

#[test]
fn missing_version_is_reported_with_field_path() {
    let message = expect_error("task_id: t\nexecution:\n  executor: custom\n  handler: h\n");
    assert!(
        message.contains("task.version is required"),
        "message = {}",
        message
    );
}

#[test]
fn zero_timeout_is_rejected() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\ntimeout_seconds: 0\nexecution:\n  executor: custom\n  handler: h\n",
    );
    assert!(
        message.contains("task.timeout_seconds must be > 0"),
        "message = {}",
        message
    );
}

#[test]
fn negative_timeout_is_rejected() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\ntimeout_seconds: -5\nexecution:\n  executor: custom\n  handler: h\n",
    );
    assert!(
        message.contains("task.timeout_seconds must be > 0"),
        "message = {}",
        message
    );
}

#[test]
fn missing_execution_is_rejected() {
    let message = expect_error("task_id: t\nversion: \"1\"\n");
    assert!(
        message.contains("task.execution is required"),
        "message = {}",
        message
    );
}

#[test]
fn unknown_executor_is_rejected() {
    let message =
        expect_error("task_id: t\nversion: \"1\"\nexecution:\n  executor: shell\n  handler: h\n");
    assert!(
        message.contains("execution.executor must be one of [http, custom]"),
        "message = {}",
        message
    );
}

#[test]
fn executor_is_case_sensitive() {
    let message =
        expect_error("task_id: t\nversion: \"1\"\nexecution:\n  executor: HTTP\n  handler: h\n");
    assert!(message.contains("execution.executor must be one of"), "message = {}", message);
}

#[test]
fn custom_without_handler_is_rejected() {
    let message = expect_error("task_id: t\nversion: \"1\"\nexecution:\n  executor: custom\n");
    assert!(
        message.contains("execution.handler is required"),
        "message = {}",
        message
    );
}

#[test]
fn http_without_config_is_rejected() {
    let message = expect_error("task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n");
    assert!(
        message.contains("execution.config is required"),
        "message = {}",
        message
    );
}

#[test]
fn http_without_method_is_rejected() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    url: http://x/\n",
    );
    assert!(
        message.contains("execution.config.method is required"),
        "message = {}",
        message
    );
}

#[test]
fn http_without_url_is_rejected() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: GET\n",
    );
    assert!(
        message.contains("execution.config.url is required"),
        "message = {}",
        message
    );
}

#[test]
fn http_body_source_file_requires_body_path() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: POST\n    url: http://x/\n    body_source: file\n",
    );
    assert!(
        message.contains("execution.config.body_path is required"),
        "message = {}",
        message
    );
}

#[test]
fn invalid_body_source_is_rejected() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: POST\n    url: http://x/\n    body_source: stream\n",
    );
    assert!(
        message.contains("execution.config.body_source must be one of"),
        "message = {}",
        message
    );
}

#[test]
fn empty_body_source_is_rejected() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: POST\n    url: http://x/\n    body_source: \"\"\n",
    );
    assert!(
        message.contains("execution.config.body_source must be one of [file, inline, none]"),
        "the empty string must not be treated as none, message = {}",
        message
    );
}

#[test]
fn unknown_top_level_field_is_rejected_with_field_path() {
    let message =
        expect_error("task_id: t\nversion: \"1\"\nenabled: true\nexecution:\n  executor: custom\n  handler: h\n");
    assert!(
        message.contains("task.enabled is not allowed"),
        "message = {}",
        message
    );
    let err = load_spec(
        "task_id: t\nversion: \"1\"\nenabled: true\nexecution:\n  executor: custom\n  handler: h\n",
    )
    .unwrap_err();
    assert_eq!(err.kind(), SpecErrorKind::Validation);
}

#[test]
fn unknown_execution_field_is_rejected_with_field_path() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: custom\n  handler: h\n  timeout: 5\n",
    );
    assert!(
        message.contains("execution.timeout is not allowed"),
        "message = {}",
        message
    );
}

#[test]
fn unknown_http_config_field_is_rejected_with_field_path() {
    let message = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: POST\n    url: http://x/\n    retries: 3\n",
    );
    assert!(
        message.contains("execution.config.retries is not allowed"),
        "message = {}",
        message
    );

    let nested = expect_error(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: POST\n    url: http://x/\n    success_check:\n      status: 200\n",
    );
    assert!(
        nested.contains("execution.config.success_check.status is not allowed"),
        "message = {}",
        nested
    );
}

#[test]
fn method_is_normalized_to_upper_case() {
    let spec = load_spec(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: post\n    url: http://x/\n",
    )
    .expect("valid");
    match spec.execution {
        Execution::Http(cfg) => assert_eq!(cfg.method, "POST"),
        _ => panic!("expected http"),
    }
}

#[test]
fn malformed_yaml_reports_parse_error() {
    let err = load_spec("task_id: [unclosed\n").unwrap_err();
    assert_eq!(err.kind(), SpecErrorKind::Parse);
}

#[test]
fn missing_file_reports_io_error() {
    let err = load_spec("/definitely/not/here/spec.yaml").unwrap_err();
    assert_eq!(err.kind(), SpecErrorKind::Io);
    assert!(err.to_string().contains("failed to read task spec"));
}

#[test]
fn file_path_load_remembers_base_dir_for_body_path() {
    let dir = tempfile::tempdir().expect("temp dir");
    let body_path = dir.path().join("payload.json");
    std::fs::write(&body_path, "{\"a\":1}").expect("write body");
    let spec_path = dir.path().join("spec.yaml");
    std::fs::write(
        &spec_path,
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: POST\n    url: http://x/\n    body_source: file\n    body_path: payload.json\n",
    )
    .expect("write spec");

    let spec: TaskSpec = load_spec(&spec_path.to_string_lossy()).expect("load spec from file");
    assert_eq!(spec.base_dir(), Some(dir.path()));

    let resolved = spec.resolve_body_path("payload.json");
    assert_eq!(resolved, body_path);
    assert_eq!(std::fs::read_to_string(&resolved).unwrap(), "{\"a\":1}");

    // Absolute paths are returned as-is.
    let absolute = dir.path().join("other.json");
    assert!(absolute.is_absolute());
    assert_eq!(
        spec.resolve_body_path(&absolute.to_string_lossy()),
        absolute
    );
}

#[test]
fn string_load_uses_working_directory_for_relative_body_path() {
    let spec = load_spec(
        "task_id: t\nversion: \"1\"\nexecution:\n  executor: http\n  config:\n    method: POST\n    url: http://x/\n    body_source: inline\n    body: '{\"a\":1}'\n",
    )
    .expect("valid");
    assert!(spec.base_dir().is_none());
    assert_eq!(
        spec.resolve_body_path("payload.json"),
        std::path::PathBuf::from("payload.json")
    );
}

#[test]
fn env_substitution_replaces_known_variables() {
    std::env::set_var("STARTUP_TASK_SDK_TEST_TOKEN", "s3cr3t");
    let out = substitute_env("Bearer ${STARTUP_TASK_SDK_TEST_TOKEN}").expect("substitute");
    assert_eq!(out, "Bearer s3cr3t");
    std::env::remove_var("STARTUP_TASK_SDK_TEST_TOKEN");
}

#[test]
fn env_substitution_reports_missing_variable() {
    std::env::remove_var("STARTUP_TASK_SDK_ABSENT");
    let err = substitute_env("${STARTUP_TASK_SDK_ABSENT}").unwrap_err();
    assert_eq!(
        err.to_string(),
        "environment variable \"STARTUP_TASK_SDK_ABSENT\" is not set"
    );
}

#[test]
fn env_substitution_ignores_non_matching_placeholders() {
    assert_eq!(substitute_env("${1BAD}").unwrap(), "${1BAD}");
    assert_eq!(substitute_env("${}").unwrap(), "${}");
    assert_eq!(substitute_env("plain $ text").unwrap(), "plain $ text");
    assert_eq!(substitute_env("${UNCLOSED").unwrap(), "${UNCLOSED");
}
