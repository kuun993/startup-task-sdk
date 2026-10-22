//! HTTP executor (contract §6.1 / §6.3).

use std::fmt;
use std::time::Duration;

use crate::spec::{BodySource, HttpConfig, TaskSpec};

/// Executor error. Its `Display` text becomes `RunResult.error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecError {
    message: String,
}

impl ExecError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ExecError {}

/// `${NAME}` is replaced with the environment variable `NAME`, which matches `[A-Za-z_][A-Za-z0-9_]*`.
///
/// A missing variable yields `environment variable "NAME" is not set` (contract §6.3).
pub fn substitute_env(input: &str) -> Result<String, ExecError> {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(rel_end) = input[i + 2..].find('}') {
                let name = &input[i + 2..i + 2 + rel_end];
                if is_valid_env_name(name) {
                    match std::env::var(name) {
                        Ok(value) => out.push_str(&value),
                        Err(_) => {
                            return Err(ExecError::new(format!(
                                "environment variable \"{}\" is not set",
                                name
                            )))
                        }
                    }
                    i = i + 2 + rel_end + 1;
                    continue;
                }
            }
        }

        // Not a placeholder: copy one whole character verbatim (keeps UTF-8 boundaries intact).
        let ch = input[i..].chars().next().expect("index is on a char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }

    Ok(out)
}

fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Execute an `executor: http` task. Success is `Ok(())`; everything else is an error (the coordinator turns it into FAILED).
pub async fn execute_http(spec: &TaskSpec, cfg: &HttpConfig) -> Result<(), ExecError> {
    let url = substitute_env(&cfg.url)?;

    let method_text = cfg.method.to_uppercase();
    let method = reqwest::Method::from_bytes(method_text.as_bytes())
        .map_err(|_| ExecError::new(format!("invalid http method \"{}\"", method_text)))?;

    let mut headers = reqwest::header::HeaderMap::new();
    for (name, raw_value) in &cfg.headers {
        let value = substitute_env(raw_value)?;
        let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
            ExecError::new(format!("invalid http header name \"{}\": {}", name, e))
        })?;
        let header_value = reqwest::header::HeaderValue::from_str(&value).map_err(|e| {
            ExecError::new(format!("invalid http header value for \"{}\": {}", name, e))
        })?;
        headers.insert(header_name, header_value);
    }

    let body: Option<Vec<u8>> = match cfg.body_source {
        BodySource::None => None,
        BodySource::Inline => {
            let raw = cfg.body.clone().unwrap_or_default();
            Some(substitute_env(&raw)?.into_bytes())
        }
        BodySource::File => {
            let path_text = cfg.body_path.clone().unwrap_or_default();
            let path = spec.resolve_body_path(&path_text);
            let bytes = std::fs::read(&path).map_err(|e| {
                ExecError::new(format!(
                    "failed to read body file \"{}\": {}",
                    path.display(),
                    e
                ))
            })?;
            Some(bytes)
        }
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(spec.timeout_seconds))
        .build()
        .map_err(|e| ExecError::new(format!("http client failed: {}", e)))?;

    let mut request = client.request(method, url.as_str()).headers(headers);
    if let Some(body) = body {
        request = request.body(body);
    }

    let response = request
        .send()
        .await
        .map_err(|e| ExecError::new(format!("http request failed: {}", e)))?;

    let status = response.status().as_u16();
    if !cfg.success_status.contains(&status) {
        let body_text = response.text().await.unwrap_or_default();
        return Err(ExecError::new(format!(
            "http status {} not in {} (body: {})",
            status,
            format_statuses(&cfg.success_status),
            body_text
        )));
    }

    Ok(())
}

fn format_statuses(statuses: &[u16]) -> String {
    let joined = statuses
        .iter()
        .map(|code| code.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{}]", joined)
}
