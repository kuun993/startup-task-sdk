"""YAML parsing / validation (design.md §6 and spec/schema.json)."""

from __future__ import annotations

from pathlib import Path

import pytest

from startup_task import SpecError, load_spec, parse_spec

MINIMAL = """
task_id: demo/task
version: "1"
execution:
  executor: custom
  handler: do_it
"""

HTTP = """
task_id: demo/http
version: "2"
execution:
  executor: http
  config:
    method: post
    url: "http://example.invalid/api"
"""


def test_minimal_custom_spec_defaults():
    spec = parse_spec(MINIMAL)

    assert spec.task_id == "demo/task"
    assert spec.version == "1"
    assert spec.description is None
    assert spec.timeout_seconds == 300
    assert spec.execution.executor == "custom"
    assert spec.execution.handler == "do_it"
    assert dict(spec.execution.config) == {}
    assert spec.execution.http is None


def test_http_spec_normalization():
    spec = parse_spec(HTTP)

    http = spec.execution.http
    assert http is not None
    assert http.method == "POST"  # upper-cased before it reaches the HTTP client
    assert http.url == "http://example.invalid/api"
    assert http.headers == {}
    assert http.body_source == "none"
    assert http.body_path is None
    assert http.success_status == (200,)
    assert http.success_status_text == "[200]"


def test_http_full_options():
    spec = parse_spec(
        """
task_id: demo/http-full
version: "3"
description: publish
timeout_seconds: 7
execution:
  executor: http
  config:
    method: PUT
    url: "https://${HOST}/x"
    headers:
      Authorization: "Bearer ${TOKEN}"
    body_source: inline
    body: '{"a":1}'
    success_check:
      http_status: [200, 201, 204]
"""
    )

    http = spec.execution.http
    assert spec.description == "publish"
    assert spec.timeout_seconds == 7
    assert http.method == "PUT"
    assert dict(http.headers) == {"Authorization": "Bearer ${TOKEN}"}
    assert http.body_source == "inline"
    assert http.body == '{"a":1}'
    assert http.success_status == (200, 201, 204)
    assert http.success_status_text == "[200, 201, 204]"


def test_success_check_scalar_status():
    spec = parse_spec(
        """
task_id: t
version: "1"
execution:
  executor: http
  config:
    method: GET
    url: "http://x"
    success_check:
      http_status: 201
"""
    )
    assert spec.execution.http.success_status == (201,)


def test_inline_body_defaults_to_empty_string():
    spec = parse_spec(
        """
task_id: t
version: "1"
execution:
  executor: http
  config:
    method: POST
    url: "http://x"
    body_source: inline
"""
    )
    assert spec.execution.http.body == ""


def test_custom_config_is_copied():
    text = """
task_id: t
version: "1"
execution:
  executor: custom
  handler: h
  config:
    batch_size: 100
    tags: [a, b]
"""
    first = parse_spec(text)
    second = parse_spec(text)
    assert first.execution.config == {"batch_size": 100, "tags": ["a", "b"]}
    assert first.execution.config is not second.execution.config
    assert first.execution.config["tags"] is not second.execution.config["tags"]


def test_load_spec_from_text_uses_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    spec = load_spec(MINIMAL)
    assert spec.base_dir == Path.cwd()


def test_empty_description_and_inline_body_are_allowed():
    spec = parse_spec(
        """
task_id: t
version: "1"
description: ""
execution:
  executor: http
  config:
    method: POST
    url: "http://x"
    body_source: inline
    body: ""
"""
    )

    assert spec.description == ""
    assert spec.execution.http.body_source == "inline"
    assert spec.execution.http.body == ""


def test_empty_body_path_rejected_for_file_body_source():
    with pytest.raises(SpecError) as excinfo:
        parse_spec(
            """
task_id: t
version: "1"
execution:
  executor: http
  config:
    method: POST
    url: "http://x"
    body_source: file
    body_path: ""
"""
        )

    message = str(excinfo.value)
    assert "execution.config.body_path" in message
    assert "required" in message


def test_empty_body_path_is_ignored_when_body_is_not_a_file():
    spec = parse_spec(
        """
task_id: t
version: "1"
execution:
  executor: http
  config:
    method: POST
    url: "http://x"
    body_source: none
    body_path: ""
"""
    )

    assert spec.execution.http.body_source == "none"
    assert spec.execution.http.body_path == ""


def test_load_spec_from_path_uses_file_directory(tmp_path):
    spec_file = tmp_path / "sub" / "task.yaml"
    spec_file.parent.mkdir()
    spec_file.write_text(HTTP, encoding="utf-8")

    spec = load_spec(spec_file)
    assert spec.task_id == "demo/http"
    assert spec.base_dir == spec_file.parent.resolve()

    as_str = load_spec(str(spec_file))
    assert as_str.base_dir == spec_file.parent.resolve()


def test_load_spec_accepts_relative_path(tmp_path, monkeypatch):
    spec_file = tmp_path / "task.yaml"
    spec_file.write_text(MINIMAL, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    spec = load_spec("task.yaml")
    assert spec.task_id == "demo/task"
    assert spec.base_dir == tmp_path.resolve()


def test_load_spec_missing_file(tmp_path):
    with pytest.raises(SpecError) as excinfo:
        load_spec(str(tmp_path / "nope.yaml"))
    assert "not found" in str(excinfo.value)


def test_load_spec_invalid_yaml():
    with pytest.raises(SpecError) as excinfo:
        load_spec("task_id: [unclosed\nversion: 1\n")
    assert "invalid YAML" in str(excinfo.value)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", "task must be a mapping"),
        ("- a\n- b\n", "task must be a mapping"),
        ("just a string", "task must be a mapping"),
        (
            "version: '1'\nexecution:\n  executor: custom\n  handler: h\n",
            "task.task_id is required",
        ),
        (
            "task_id: ''\nversion: '1'\nexecution:\n  executor: custom\n  handler: h\n",
            "task.task_id must not be empty",
        ),
        (
            "task_id: 'a b'\nversion: '1'\nexecution:\n  executor: custom\n  handler: h\n",
            "task.task_id must match",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: custom\n  handler: h\n"
            "  extra: 1\n",
            "execution.extra is not allowed",
        ),
        (
            "task_id: t\nversion: '1'\nextra: 1\nexecution:\n"
            "  executor: custom\n  handler: h\n",
            "task.extra is not allowed",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: shell\n  handler: h\n",
            "execution.executor must be one of",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: custom\n",
            "execution.handler is required",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config: 1\n",
            "execution.config must be a mapping",
        ),
    ],
)
def test_validation_errors_contain_field_path(text, expected):
    with pytest.raises(SpecError) as excinfo:
        parse_spec(text)
    assert expected in str(excinfo.value)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "task_id: t\nversion: '1'\ntimeout_seconds: 0\n"
            "execution:\n  executor: custom\n  handler: h\n",
            "task.timeout_seconds must be > 0",
        ),
        (
            "task_id: t\nversion: '1'\ntimeout_seconds: -5\n"
            "execution:\n  executor: custom\n  handler: h\n",
            "task.timeout_seconds must be > 0",
        ),
        (
            "task_id: t\nversion: '1'\ntimeout_seconds: true\n"
            "execution:\n  executor: custom\n  handler: h\n",
            "task.timeout_seconds must be an integer",
        ),
        (
            "task_id: t\nversion: '1'\ntimeout_seconds: 1.5\n"
            "execution:\n  executor: custom\n  handler: h\n",
            "task.timeout_seconds must be an integer",
        ),
        (
            "task_id: t\nversion: '1'\n"
            "execution:\n  executor: custom\n  handler: ''\n",
            "execution.handler must not be empty",
        ),
        (
            "task_id: t\nversion: '1'\n"
            "execution:\n  executor: custom\n  handler: h\n  config: [1, 2]\n",
            "execution.config must be a mapping",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n",
            "execution.config is required",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n",
            "execution.config.url is required",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    url: http://x\n",
            "execution.config.method is required",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    bogus: 1\n",
            "execution.config.bogus is not allowed",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    headers: notamap\n",
            "execution.config.headers must be a mapping",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    headers:\n      X-N: 1\n",
            "execution.config.headers.X-N must be a string",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    body_source: file\n",
            "execution.config.body_path is required",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    body_source: stdin\n",
            "execution.config.body_source must be one of",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    success_check:\n"
            "      http_status: []\n",
            "execution.config.success_check.http_status must not be an empty list",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    success_check:\n"
            "      http_status: '200'\n",
            "must be an integer or a list of integers",
        ),
        (
            "task_id: t\nversion: '1'\nexecution:\n  executor: http\n  config:\n"
            "    method: POST\n    url: http://x\n    success_check:\n"
            "      other: 1\n",
            "execution.config.success_check.other is not allowed",
        ),
        (
            "task_id: t\nversion: '1'\ndescription: 5\n"
            "execution:\n  executor: custom\n  handler: h\n",
            "task.description must be a string",
        ),
        (
            "task_id: t\nversion: '1'\nexecution: custom\n",
            "execution must be a mapping",
        ),
        (
            "task_id: t\nversion: ''\nexecution:\n  executor: custom\n  handler: h\n",
            "task.version must not be empty",
        ),
    ],
)
def test_more_validation_errors(text, expected):
    with pytest.raises(SpecError) as excinfo:
        parse_spec(text)
    assert expected in str(excinfo.value)


def test_task_id_and_version_length_limits():
    long_id = "a" * 129
    with pytest.raises(SpecError) as excinfo:
        parse_spec(
            f"task_id: {long_id}\nversion: '1'\n"
            "execution:\n  executor: custom\n  handler: h\n"
        )
    assert "task.task_id must be at most 128 characters" in str(excinfo.value)

    long_version = "b" * 65
    with pytest.raises(SpecError) as excinfo:
        parse_spec(
            f"task_id: t\nversion: '{long_version}'\n"
            "execution:\n  executor: custom\n  handler: h\n"
        )
    assert "task.version must be at most 64 characters" in str(excinfo.value)


def test_spec_error_is_value_error():
    assert issubclass(SpecError, ValueError)
    with pytest.raises(ValueError):
        parse_spec("task_id: t\n")
