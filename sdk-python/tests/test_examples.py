"""The shipped ``examples/`` directory (same files in all four SDKs)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from startup_task import Coordinator, Outcome, SqliteStore, ensure_schema, load_spec
from startup_task import executor as executor_module

EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "examples"
PUBLISH_SPEC = EXAMPLE_DIR / "publish_app_settings.yaml"
SETTINGS_BODY = EXAMPLE_DIR / "app_settings.json"
WARM_UP_SPEC = EXAMPLE_DIR / "warm_up_cache.yaml"
RUN_SCRIPT = EXAMPLE_DIR / "run_examples.py"

WARM_UP_CONFIG = {
    "keys": ["home:feed", "home:recommend", "config:feature-flags"],
    "ttl_seconds": 300,
}

PUBLISH_DESCRIPTION = (
    "Publish application default settings to the config center"
)
WARM_UP_DESCRIPTION = "Warm up hot data in the cache"


def _assert_example_description(actual: str, expected: str) -> None:
    """Pin the documented English description of a shared example spec.

    ``examples/*.yaml`` is owned by the cross-language example set and is
    translated separately from this SDK. While a description is still non-ASCII
    only its presence is required; as soon as the English text lands (pure
    ASCII, like every documented value) this becomes a strict equality check.
    """
    assert actual.strip(), "example spec must carry a description"
    if actual.isascii():
        assert actual == expected


def test_examples_directory_contains_only_the_shared_files():
    assert PUBLISH_SPEC.is_file()
    assert SETTINGS_BODY.is_file()
    assert WARM_UP_SPEC.is_file()
    assert RUN_SCRIPT.is_file()

    files = sorted(path.name for path in EXAMPLE_DIR.iterdir() if path.is_file())
    assert files == [
        "app_settings.json",
        "publish_app_settings.yaml",
        "run_examples.py",
        "warm_up_cache.yaml",
    ], f"unexpected files in examples/: {files}"


def test_publish_app_settings_spec_parses():
    spec = load_spec(PUBLISH_SPEC)
    http = spec.execution.http

    assert spec.task_id == "config/publish-app-settings"
    assert spec.version == "2026-10-22-v1"
    assert spec.description is not None
    _assert_example_description(spec.description, PUBLISH_DESCRIPTION)
    assert spec.timeout_seconds == 120
    assert spec.execution.executor == "http"
    assert spec.base_dir == EXAMPLE_DIR

    assert http is not None
    assert http.method == "PUT"
    assert http.url == "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"
    assert dict(http.headers) == {
        "Authorization": "Bearer ${CONFIG_CENTER_TOKEN}",
        "Content-Type": "application/json",
    }
    assert http.body_source == "file"
    assert http.body_path == "app_settings.json"
    assert http.success_status == (200, 201)


def test_app_settings_body_matches_the_documented_payload():
    body = json.loads(SETTINGS_BODY.read_text(encoding="utf-8"))

    assert body == {
        "feature_flags": {"new_checkout": True, "dark_mode": False},
        "limits": {"max_upload_mb": 50, "rate_limit_per_minute": 600},
        "updated_by": "startup-task-sdk",
    }


def test_warm_up_cache_spec_parses():
    spec = load_spec(WARM_UP_SPEC)

    assert spec.task_id == "cache/warm-up"
    assert spec.version == "2026-10-22-v1"
    assert spec.description is not None
    _assert_example_description(spec.description, WARM_UP_DESCRIPTION)
    assert spec.timeout_seconds == 60
    assert spec.execution.executor == "custom"
    assert spec.execution.handler == "warm_up_cache"
    assert dict(spec.execution.config) == WARM_UP_CONFIG


def test_warm_up_cache_example_runs_once(db_path):
    spec = load_spec(WARM_UP_SPEC)
    store = SqliteStore(db_path)
    ensure_schema(store)

    seen: list[dict] = []

    def warm_up_cache(config: dict) -> None:
        seen.append(config)

    coordinator = Coordinator(store, owner_id="example-pod")
    coordinator.register_handler("warm_up_cache", warm_up_cache)

    assert coordinator.run(spec).outcome is Outcome.EXECUTED
    assert seen == [WARM_UP_CONFIG]
    assert coordinator.run(spec).outcome is Outcome.SKIPPED_ALREADY_DONE
    assert len(seen) == 1


def test_publish_app_settings_example_executes(db_path, monkeypatch):
    monkeypatch.setenv("CONFIG_CENTER_URL", "http://config-center.example")
    monkeypatch.setenv("CONFIG_CENTER_TOKEN", "token-123")
    monkeypatch.setenv("APP_NAME", "demo-app")

    captured: dict = {}

    class _Response:
        status_code = 200
        text = "ok"
        content = b"ok"

    def fake_request(method, url, **kwargs):
        captured.update(method=method, url=url, **kwargs)
        return _Response()

    monkeypatch.setattr(executor_module.requests, "request", fake_request)

    store = SqliteStore(db_path)
    ensure_schema(store)
    coordinator = Coordinator(store, owner_id="example-pod")

    result = coordinator.run(load_spec(PUBLISH_SPEC))

    assert result.outcome is Outcome.EXECUTED, result.error
    assert captured["method"] == "PUT"
    assert captured["url"] == (
        "http://config-center.example/api/v1/apps/demo-app/settings"
    )
    assert captured["headers"]["Authorization"] == "Bearer token-123"
    assert captured["headers"]["Content-Type"] == "application/json"
    # body_source=file resolved relative to the spec directory
    assert json.loads(captured["data"]) == json.loads(
        SETTINGS_BODY.read_text(encoding="utf-8")
    )


def test_run_examples_script_exits_zero():
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("CONFIG_CENTER_") and key != "APP_NAME"
    }

    completed = subprocess.run(
        [sys.executable, str(RUN_SCRIPT)],
        cwd=str(EXAMPLE_DIR.parent),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert completed.returncode == 0, completed.stderr
    assert "config/publish-app-settings@2026-10-22-v1 -> FAILED" in completed.stdout
    assert "cache/warm-up@2026-10-22-v1 -> EXECUTED" in completed.stdout
    assert "SKIPPED_ALREADY_DONE" in completed.stdout
    assert "the config-center task failed as expected on this machine" in completed.stdout
    assert "traceback" not in completed.stderr.lower()
