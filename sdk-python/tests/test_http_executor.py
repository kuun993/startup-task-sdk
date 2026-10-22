"""The built-in http executor against a real local HTTP server."""

from __future__ import annotations

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

from startup_task import Coordinator, Outcome, SqliteStore, ensure_schema, load_spec
from startup_task import executor as executor_module


class _Server:
    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.status = 200

    def handler(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _reply(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                server.requests.append(
                    {
                        "method": self.command,
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "body": body,
                    }
                )
                payload = b"ok"
                self.send_response(server.status)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = _reply
            do_POST = _reply
            do_PUT = _reply
            do_PATCH = _reply
            do_DELETE = _reply

            def log_message(self, *args) -> None:  # keep pytest output clean
                pass

        return Handler


@pytest.fixture()
def server():
    state = _Server()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), state.handler())
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        yield SimpleNamespace(
            base_url=base_url,
            requests=state.requests,
            set_status=lambda code: setattr(state, "status", code),
        )
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


@pytest.fixture()
def http_coordinator(db_path):
    store = SqliteStore(db_path)
    ensure_schema(store)
    return Coordinator(store, owner_id="http-pod")


def _http_spec(text: str | Path):
    return load_spec(text)


def test_http_executed_with_env_substitution(tmp_path, http_coordinator, server, monkeypatch):
    monkeypatch.setenv("CONFIG_CENTER_URL", server.base_url)
    monkeypatch.setenv("CONFIG_CENTER_TOKEN", "s3cr3t")
    monkeypatch.setenv("APP_NAME", "demo-app")

    spec = _http_spec(
        """
task_id: config/test
version: "1"
execution:
  executor: http
  config:
    method: PUT
    url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"
    headers:
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"
      X-App-Name: "${APP_NAME}"
    body_source: inline
    body: '{"app":"${APP_NAME}"}'
    success_check:
      http_status: [200, 201]
"""
    )

    result = http_coordinator.run(spec)

    assert result.outcome is Outcome.EXECUTED
    assert result.error is None
    assert len(server.requests) == 1
    request = server.requests[0]
    assert request["method"] == "PUT"
    assert request["path"] == "/api/v1/apps/demo-app/settings"
    assert request["headers"]["authorization"] == "Bearer s3cr3t"
    assert request["headers"]["x-app-name"] == "demo-app"
    assert request["body"] == b'{"app":"demo-app"}'


def test_http_inline_empty_body_executes(http_coordinator, server, monkeypatch):
    monkeypatch.setenv("TARGET", server.base_url)

    result = http_coordinator.run(
        _http_spec(
            """
task_id: config/empty-body
version: "1"
execution:
  executor: http
  config:
    method: POST
    url: "${TARGET}/api"
    body_source: inline
    body: ""
"""
        )
    )

    assert result.outcome is Outcome.EXECUTED
    assert len(server.requests) == 1
    assert server.requests[0]["body"] == b""


def test_http_body_from_file_relative_to_spec(tmp_path, db_path, server, monkeypatch):
    monkeypatch.setenv("CONFIG_CENTER_URL", server.base_url)
    monkeypatch.setenv("CONFIG_CENTER_TOKEN", "t")
    monkeypatch.chdir(tmp_path)  # prove resolution is relative to the spec file

    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    payload = spec_dir / "app_settings.json"
    payload.write_text('{"app":"from-file"}', encoding="utf-8")

    spec_file = spec_dir / "publish.yaml"
    spec_file.write_text(
        """
task_id: config/file
version: "1"
execution:
  executor: http
  config:
    method: PUT
    url: "${CONFIG_CENTER_URL}/api/v1/apps/demo-app/settings"
    headers:
      Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"
    body_source: file
    body_path: app_settings.json
""",
        encoding="utf-8",
    )

    store = SqliteStore(db_path)
    ensure_schema(store)
    coordinator = Coordinator(store, owner_id="http-pod")

    result = coordinator.run(load_spec(spec_file))

    assert result.outcome is Outcome.EXECUTED
    assert server.requests[0]["method"] == "PUT"
    assert server.requests[0]["body"] == payload.read_bytes()


def test_http_missing_env_var_fails(http_coordinator, server, monkeypatch):
    monkeypatch.delenv("MISSING_URL", raising=False)

    result = http_coordinator.run(
        _http_spec(
            """
task_id: config/env
version: "1"
execution:
  executor: http
  config:
    method: GET
    url: "${MISSING_URL}/api"
"""
        )
    )

    assert result.outcome is Outcome.FAILED
    assert result.error == 'environment variable "MISSING_URL" is not set'
    assert server.requests == []


def test_http_unexpected_status_fails(http_coordinator, server, monkeypatch):
    monkeypatch.setenv("TARGET", server.base_url)
    server.set_status(500)

    result = http_coordinator.run(
        _http_spec(
            """
task_id: config/status
version: "1"
execution:
  executor: http
  config:
    method: GET
    url: "${TARGET}/api"
"""
        )
    )

    assert result.outcome is Outcome.FAILED
    assert result.error == "http status 500 not in [200] (body: ok)"


def test_http_expected_status_list(http_coordinator, server, monkeypatch):
    monkeypatch.setenv("TARGET", server.base_url)
    server.set_status(201)

    result = http_coordinator.run(
        _http_spec(
            """
task_id: config/created
version: "1"
execution:
  executor: http
  config:
    method: POST
    url: "${TARGET}/api"
    success_check:
      http_status: [200, 201]
"""
        )
    )

    assert result.outcome is Outcome.EXECUTED


def test_http_transport_error_fails(http_coordinator):
    # Bind then release a port so nothing is listening on it.
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    result = http_coordinator.run(
        _http_spec(
            f"""
task_id: config/down
version: "1"
timeout_seconds: 5
execution:
  executor: http
  config:
    method: GET
    url: "http://127.0.0.1:{port}/api"
"""
        )
    )

    assert result.outcome is Outcome.FAILED
    assert (result.error or "").startswith("http request failed: ")


def test_http_uses_timeout_seconds(tmp_path, http_coordinator, monkeypatch):
    captured: dict = {}

    class _Response:
        status_code = 200
        text = "ok"
        content = b"ok"

    def fake_request(method, url, **kwargs):
        captured.update(method=method, url=url, **kwargs)
        return _Response()

    monkeypatch.setattr(executor_module.requests, "request", fake_request)

    result = http_coordinator.run(
        _http_spec(
            """
task_id: config/timeout
version: "1"
timeout_seconds: 17
execution:
  executor: http
  config:
    method: get
    url: "http://example.invalid/x"
"""
        )
    )

    assert result.outcome is Outcome.EXECUTED
    assert captured["method"] == "GET"
    assert captured["timeout"] == 17
    assert captured["data"] is None


def test_http_missing_body_file_fails(http_coordinator):
    result = http_coordinator.run(
        _http_spec(
            """
task_id: config/nofile
version: "1"
execution:
  executor: http
  config:
    method: POST
    url: "http://127.0.0.1:1/api"
    body_source: file
    body_path: definitely-missing.json
"""
        )
    )

    assert result.outcome is Outcome.FAILED
    assert "cannot read body file" in (result.error or "")
