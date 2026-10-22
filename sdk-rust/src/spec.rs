//! Parsing and validation of YAML task definitions (contract §6).

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const MAX_TASK_ID_LEN: usize = 128;
const MAX_VERSION_LEN: usize = 64;
const DEFAULT_TIMEOUT_SECONDS: u64 = 300;

/// Parse/validation error. Messages always carry the field path for easy location (contract §6.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecError {
    kind: SpecErrorKind,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecErrorKind {
    /// Reading the spec file failed.
    Io,
    /// YAML/JSON deserialization failed.
    Parse,
    /// A field is missing, out of range or otherwise invalid.
    Validation,
}

impl SpecError {
    pub fn kind(&self) -> SpecErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    fn io(message: impl Into<String>) -> Self {
        Self {
            kind: SpecErrorKind::Io,
            message: message.into(),
        }
    }

    fn parse(message: impl Into<String>) -> Self {
        Self {
            kind: SpecErrorKind::Parse,
            message: message.into(),
        }
    }

    fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: SpecErrorKind::Validation,
            message: message.into(),
        }
    }
}

impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SpecError {}

/// Top-level task definition.
#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub task_id: String,
    pub version: String,
    pub description: Option<String>,
    pub timeout_seconds: u64,
    pub execution: Execution,
    /// Directory remembered when the spec is loaded from a file, used to resolve a relative `body_path`.
    base_dir: Option<PathBuf>,
}

impl TaskSpec {
    /// Directory of the spec file when loaded from a path; `None` when loaded from a string.
    pub fn base_dir(&self) -> Option<&Path> {
        self.base_dir.as_deref()
    }

    /// Resolve `body_path`: absolute paths are returned as-is; relative ones are resolved against the spec file directory, else the process working directory.
    pub fn resolve_body_path(&self, body_path: &str) -> PathBuf {
        let p = Path::new(body_path);
        if p.is_absolute() {
            return p.to_path_buf();
        }
        match &self.base_dir {
            Some(dir) => dir.join(p),
            None => p.to_path_buf(),
        }
    }
}

/// How the task is executed.
#[derive(Debug, Clone)]
pub enum Execution {
    Http(HttpConfig),
    Custom(CustomConfig),
}

impl Execution {
    /// `"http"` or `"custom"`, matching `execution.executor` in the YAML.
    pub fn executor(&self) -> &'static str {
        match self {
            Execution::Http(_) => "http",
            Execution::Custom(_) => "custom",
        }
    }
}

/// Configuration of `executor: http`.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body_source: BodySource,
    pub body_path: Option<String>,
    pub body: Option<String>,
    /// Status codes considered successful; defaults to `[200]`.
    pub success_status: Vec<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodySource {
    None,
    File,
    Inline,
}

impl BodySource {
    pub fn as_str(&self) -> &'static str {
        match self {
            BodySource::None => "none",
            BodySource::File => "file",
            BodySource::Inline => "inline",
        }
    }
}

/// Configuration of `executor: custom`. `config` is handed to the caller handler as-is (deep copy).
#[derive(Debug, Clone)]
pub struct CustomConfig {
    pub handler: String,
    pub config: serde_json::Value,
}

/// Load a task definition: `src` may be YAML text or a file path.
pub fn load_spec(src: &str) -> Result<TaskSpec, SpecError> {
    let path = Path::new(src);
    if path.is_file() {
        let text = std::fs::read_to_string(path).map_err(|e| {
            SpecError::io(format!("failed to read task spec \"{}\": {}", src, e))
        })?;
        let base_dir = path.parent().map(|p| p.to_path_buf());
        return parse_spec(&text, base_dir);
    }

    // Looks like a file path but does not exist: report a clear IO error instead of parsing the path as YAML.
    if !src.contains('\n') {
        let lower = src.to_ascii_lowercase();
        if lower.ends_with(".yaml") || lower.ends_with(".yml") || lower.ends_with(".json") {
            return Err(SpecError::io(format!(
                "failed to read task spec \"{}\": no such file",
                src
            )));
        }
    }

    parse_spec(src, None)
}

fn parse_spec(text: &str, base_dir: Option<PathBuf>) -> Result<TaskSpec, SpecError> {
    let document: serde_yaml::Value = serde_yaml::from_str(text)
        .map_err(|e| SpecError::parse(format!("invalid YAML: {}", e)))?;
    reject_unknown_fields(&document)?;
    let raw: RawSpec = serde_yaml::from_value(document)
        .map_err(|e| SpecError::parse(format!("invalid task spec: {}", e)))?;
    validate(raw, base_dir)
}

/// Allowed fields (mirrors `additionalProperties: false` in `spec/schema.json`).
const TOP_LEVEL_FIELDS: &[&str] = &[
    "task_id",
    "version",
    "description",
    "timeout_seconds",
    "execution",
];
const EXECUTION_FIELDS: &[&str] = &["executor", "handler", "config"];
const HTTP_CONFIG_FIELDS: &[&str] = &[
    "method",
    "url",
    "headers",
    "body_source",
    "body_path",
    "body",
    "success_check",
];
const SUCCESS_CHECK_FIELDS: &[&str] = &["http_status"];

/// Unknown fields are reported with their field path as well (`task.foo is not allowed`).
fn reject_unknown_fields(document: &serde_yaml::Value) -> Result<(), SpecError> {
    let Some(top) = document.as_mapping() else {
        // A non-mapping top level is left to serde for the type error.
        return Ok(());
    };
    check_keys(top, TOP_LEVEL_FIELDS, "task")?;

    let Some(execution) = top.get("execution").and_then(|v| v.as_mapping()) else {
        return Ok(());
    };
    check_keys(execution, EXECUTION_FIELDS, "execution")?;

    if execution.get("executor").and_then(|v| v.as_str()) != Some("http") {
        return Ok(());
    }
    let Some(config) = execution.get("config").and_then(|v| v.as_mapping()) else {
        return Ok(());
    };
    check_keys(config, HTTP_CONFIG_FIELDS, "execution.config")?;

    if let Some(success_check) = config.get("success_check").and_then(|v| v.as_mapping()) {
        check_keys(
            success_check,
            SUCCESS_CHECK_FIELDS,
            "execution.config.success_check",
        )?;
    }
    Ok(())
}

fn check_keys(
    mapping: &serde_yaml::Mapping,
    allowed: &[&str],
    path: &str,
) -> Result<(), SpecError> {
    for key in mapping.keys() {
        match key.as_str() {
            Some(name) if allowed.contains(&name) => {}
            Some(name) => {
                return Err(SpecError::validation(format!(
                    "{}.{} is not allowed",
                    path, name
                )))
            }
            None => {
                return Err(SpecError::validation(format!(
                    "{} contains a non-string field name",
                    path
                )))
            }
        }
    }
    Ok(())
}

fn validate(raw: RawSpec, base_dir: Option<PathBuf>) -> Result<TaskSpec, SpecError> {
    let task_id = raw
        .task_id
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SpecError::validation("task.task_id is required"))?;
    if task_id.chars().count() > MAX_TASK_ID_LEN {
        return Err(SpecError::validation(format!(
            "task.task_id must be at most {} characters",
            MAX_TASK_ID_LEN
        )));
    }
    if !task_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '/' | '-'))
    {
        return Err(SpecError::validation(
            "task.task_id must match ^[A-Za-z0-9._:/-]+$",
        ));
    }

    let version = raw
        .version
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SpecError::validation("task.version is required"))?;
    if version.chars().count() > MAX_VERSION_LEN {
        return Err(SpecError::validation(format!(
            "task.version must be at most {} characters",
            MAX_VERSION_LEN
        )));
    }

    let timeout_seconds = match raw.timeout_seconds {
        None => DEFAULT_TIMEOUT_SECONDS,
        Some(v) if v <= 0 => {
            return Err(SpecError::validation(
                "task.timeout_seconds must be > 0",
            ))
        }
        Some(v) => v as u64,
    };

    let execution = validate_execution(raw.execution)?;

    Ok(TaskSpec {
        task_id,
        version,
        description: raw.description,
        timeout_seconds,
        execution,
        base_dir,
    })
}

fn validate_execution(raw: Option<RawExecution>) -> Result<Execution, SpecError> {
    let raw = raw.ok_or_else(|| SpecError::validation("task.execution is required"))?;

    let executor = raw
        .executor
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SpecError::validation("execution.executor is required"))?;

    match executor.as_str() {
        "http" => {
            let config = raw
                .config
                .ok_or_else(|| SpecError::validation("execution.config is required"))?;
            if !config.is_mapping() {
                return Err(SpecError::validation(
                    "execution.config must be an object",
                ));
            }
            let http: RawHttpConfig = serde_yaml::from_value(config).map_err(|e| {
                SpecError::validation(format!("execution.config is invalid: {}", e))
            })?;
            Ok(Execution::Http(validate_http(http)?))
        }
        "custom" => {
            let handler = raw
                .handler
                .filter(|v| !v.is_empty())
                .ok_or_else(|| SpecError::validation("execution.handler is required"))?;
            let config = match raw.config {
                None => serde_json::json!({}),
                Some(value) => {
                    if !value.is_mapping() {
                        return Err(SpecError::validation(
                            "execution.config must be an object",
                        ));
                    }
                    serde_json::to_value(&value).map_err(|e| {
                        SpecError::validation(format!("execution.config is invalid: {}", e))
                    })?
                }
            };
            Ok(Execution::Custom(CustomConfig { handler, config }))
        }
        other => Err(SpecError::validation(format!(
            "execution.executor must be one of [http, custom], got \"{}\"",
            other
        ))),
    }
}

fn validate_http(raw: RawHttpConfig) -> Result<HttpConfig, SpecError> {
    let method = raw
        .method
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SpecError::validation("execution.config.method is required"))?;
    let url = raw
        .url
        .filter(|v| !v.is_empty())
        .ok_or_else(|| SpecError::validation("execution.config.url is required"))?;

    // The empty string is not a valid value (it must not fall back to the default none).
    let body_source = match raw.body_source.as_deref() {
        None | Some("none") => BodySource::None,
        Some("file") => BodySource::File,
        Some("inline") => BodySource::Inline,
        Some(other) => {
            return Err(SpecError::validation(format!(
                "execution.config.body_source must be one of [file, inline, none], got \"{}\"",
                other
            )))
        }
    };

    if body_source == BodySource::File {
        match raw.body_path.as_deref() {
            None | Some("") => {
                return Err(SpecError::validation(
                    "execution.config.body_path is required when body_source is \"file\"",
                ))
            }
            _ => {}
        }
    }

    let success_status = match raw.success_check.and_then(|c| c.http_status) {
        None => vec![200u16],
        Some(RawHttpStatus::One(code)) => vec![normalize_status(code)?],
        Some(RawHttpStatus::Many(codes)) => {
            if codes.is_empty() {
                return Err(SpecError::validation(
                    "execution.config.success_check.http_status must not be empty",
                ));
            }
            codes
                .into_iter()
                .map(normalize_status)
                .collect::<Result<Vec<u16>, SpecError>>()?
        }
    };

    let body = match body_source {
        // body_source=inline without a body uses an empty string (contract §6.1).
        BodySource::Inline => Some(raw.body.unwrap_or_default()),
        _ => raw.body,
    };

    Ok(HttpConfig {
        method: method.to_uppercase(),
        url,
        headers: raw.headers.unwrap_or_default(),
        body_source,
        body_path: raw.body_path,
        body,
        success_status,
    })
}

fn normalize_status(code: i64) -> Result<u16, SpecError> {
    u16::try_from(code).map_err(|_| {
        SpecError::validation(format!(
            "execution.config.success_check.http_status contains an invalid status code: {}",
            code
        ))
    })
}

// ---------------------------------------------------------------------------
// Deserialization layer: required fields arrive as Option so a missing one yields a validation error with its path.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSpec {
    task_id: Option<String>,
    version: Option<String>,
    description: Option<String>,
    timeout_seconds: Option<i64>,
    execution: Option<RawExecution>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawExecution {
    executor: Option<String>,
    handler: Option<String>,
    config: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHttpConfig {
    method: Option<String>,
    url: Option<String>,
    headers: Option<BTreeMap<String, String>>,
    body_source: Option<String>,
    body_path: Option<String>,
    body: Option<String>,
    success_check: Option<RawSuccessCheck>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSuccessCheck {
    http_status: Option<RawHttpStatus>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawHttpStatus {
    One(i64),
    Many(Vec<i64>),
}
