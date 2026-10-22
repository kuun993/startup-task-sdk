"""YAML task definition parsing and validation.

Implements ``docs/design.md`` §6: ``load_spec`` accepts either YAML text or a
file path and validates the whole document, raising :class:`SpecError` with a
message that always contains the offending field path.
"""

from __future__ import annotations

import copy
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

#: Field-path prefix for top level fields, as used by ``docs/design.md`` §6.4.
TOP = "task"

DEFAULT_TIMEOUT_SECONDS = 300
TASK_ID_MAX_LENGTH = 128
VERSION_MAX_LENGTH = 64
TASK_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:/-]+$")
TASK_ID_PATTERN_TEXT = "^[A-Za-z0-9._:/-]+$"

EXECUTOR_HTTP = "http"
EXECUTOR_CUSTOM = "custom"
EXECUTORS = (EXECUTOR_HTTP, EXECUTOR_CUSTOM)

BODY_SOURCE_FILE = "file"
BODY_SOURCE_INLINE = "inline"
BODY_SOURCE_NONE = "none"
BODY_SOURCES = (BODY_SOURCE_FILE, BODY_SOURCE_INLINE, BODY_SOURCE_NONE)

DEFAULT_SUCCESS_STATUS = (200,)

_TOP_LEVEL_FIELDS = frozenset(
    {"task_id", "version", "description", "timeout_seconds", "execution"}
)
_EXECUTION_FIELDS = frozenset({"executor", "handler", "config"})
_HTTP_CONFIG_FIELDS = frozenset(
    {
        "method",
        "url",
        "headers",
        "body_source",
        "body_path",
        "body",
        "success_check",
    }
)
_SUCCESS_CHECK_FIELDS = frozenset({"http_status"})

_SPEC_FILE_SUFFIXES = (".yaml", ".yml", ".json")


class SpecError(ValueError):
    """Raised when a task definition cannot be parsed or validated."""


@dataclass(frozen=True)
class HttpConfig:
    """Resolved ``execution.config`` of an ``executor: http`` task."""

    method: str
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)
    body_source: str = BODY_SOURCE_NONE
    body_path: str | None = None
    body: str | None = None
    success_status: tuple[int, ...] = DEFAULT_SUCCESS_STATUS

    @property
    def success_status_text(self) -> str:
        """Render the accepted status codes exactly as used in failure messages."""
        return "[" + ", ".join(str(code) for code in self.success_status) + "]"


@dataclass(frozen=True)
class ExecutionSpec:
    """Resolved ``execution`` block."""

    executor: str
    handler: str | None = None
    config: Mapping[str, Any] = field(default_factory=dict)
    http: HttpConfig | None = None


@dataclass(frozen=True)
class TaskSpec:
    """A validated task definition."""

    task_id: str
    version: str
    execution: ExecutionSpec
    description: str | None = None
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    #: Directory used to resolve a relative ``body_path``.
    base_dir: Path = field(default_factory=Path.cwd)


def load_spec(str_or_path: str | os.PathLike[str]) -> TaskSpec:
    """Parse and validate a task definition.

    ``str_or_path`` may be a YAML document or a path to a YAML file. A relative
    ``execution.config.body_path`` is resolved against the spec file's directory
    when a path is given, and against the process working directory otherwise.
    """
    text, base_dir = _read_source(str_or_path)
    return _parse(text, base_dir)


def parse_spec(text: str, base_dir: str | os.PathLike[str] | None = None) -> TaskSpec:
    """Parse and validate YAML text (no filesystem probing)."""
    return _parse(text, Path(base_dir) if base_dir is not None else Path.cwd())


def _read_source(str_or_path: str | os.PathLike[str]) -> tuple[str, Path]:
    if isinstance(str_or_path, os.PathLike):
        path = Path(str_or_path)
        return _read_file(path), path.resolve().parent

    if not isinstance(str_or_path, str):
        raise SpecError(
            f"spec source must be str or os.PathLike, got {type(str_or_path).__name__}"
        )

    if _is_existing_file(str_or_path):
        path = Path(str_or_path)
        return _read_file(path), path.resolve().parent

    if _looks_like_missing_file(str_or_path):
        raise SpecError(f"spec file not found: {str_or_path}")

    return str_or_path, Path.cwd()


def _is_existing_file(text: str) -> bool:
    if not text or "\n" in text or "\r" in text or len(text) > 1024:
        return False
    try:
        return os.path.isfile(text)
    except OSError:  # pragma: no cover - defensive, e.g. absurdly long paths
        return False


def _looks_like_missing_file(text: str) -> bool:
    if not text or "\n" in text or "\r" in text or len(text) > 1024:
        return False
    return text.lower().endswith(_SPEC_FILE_SUFFIXES)


def _read_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SpecError(f"cannot read spec file {path}: {exc}") from exc


def _parse(text: str, base_dir: Path) -> TaskSpec:
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise SpecError(f"invalid YAML: {exc}") from exc

    if not isinstance(document, dict):
        raise SpecError(f"{TOP} must be a mapping, got {_kind(document)}")

    _reject_unknown_fields(document, _TOP_LEVEL_FIELDS, TOP)

    task_id = _require_str(document, "task_id", TOP, max_length=TASK_ID_MAX_LENGTH)
    if not TASK_ID_PATTERN.match(task_id):
        raise SpecError(
            f"{TOP}.task_id must match {TASK_ID_PATTERN_TEXT}, got {task_id!r}"
        )
    version = _require_str(document, "version", TOP, max_length=VERSION_MAX_LENGTH)
    description = _optional_str(document, "description", TOP)
    timeout_seconds = _optional_timeout(document)

    if "execution" not in document:
        raise SpecError(f"{TOP}.execution is required")
    execution = _parse_execution(document["execution"])

    return TaskSpec(
        task_id=task_id,
        version=version,
        execution=execution,
        description=description,
        timeout_seconds=timeout_seconds,
        base_dir=base_dir,
    )


def _parse_execution(raw: Any) -> ExecutionSpec:
    path = "execution"
    if not isinstance(raw, dict):
        raise SpecError(f"{path} must be a mapping, got {_kind(raw)}")

    _reject_unknown_fields(raw, _EXECUTION_FIELDS, path)

    if "executor" not in raw:
        raise SpecError(f"{path}.executor is required")
    executor = raw["executor"]
    if not isinstance(executor, str):
        raise SpecError(f"{path}.executor must be a string, got {_kind(executor)}")
    if executor not in EXECUTORS:
        raise SpecError(
            f"{path}.executor must be one of {list(EXECUTORS)}, got {executor!r}"
        )

    handler: str | None = None
    if executor == EXECUTOR_CUSTOM:
        handler = _require_str(raw, "handler", path, max_length=None)
        config = _optional_config(raw, path)
        return ExecutionSpec(
            executor=executor,
            handler=handler,
            config=config,
            http=None,
        )

    if "config" not in raw:
        raise SpecError(f"{path}.config is required")
    http = _parse_http_config(raw["config"])
    handler_value = raw.get("handler")
    if handler_value is not None and not isinstance(handler_value, str):
        raise SpecError(
            f"{path}.handler must be a string, got {_kind(handler_value)}"
        )
    return ExecutionSpec(
        executor=executor,
        handler=handler_value,
        config=copy.deepcopy(raw["config"]),
        http=http,
    )


def _parse_http_config(raw: Any) -> HttpConfig:
    path = "execution.config"
    if not isinstance(raw, dict):
        raise SpecError(f"{path} must be a mapping, got {_kind(raw)}")

    _reject_unknown_fields(raw, _HTTP_CONFIG_FIELDS, path)

    method = _require_str(raw, "method", path, max_length=None)
    method = method.strip()
    if not method:
        raise SpecError(f"{path}.method must not be empty")
    url = _require_str(raw, "url", path, max_length=None)

    headers: dict[str, str] = {}
    if "headers" in raw and raw["headers"] is not None:
        raw_headers = raw["headers"]
        if not isinstance(raw_headers, dict):
            raise SpecError(
                f"{path}.headers must be a mapping, got {_kind(raw_headers)}"
            )
        for key, value in raw_headers.items():
            if not isinstance(key, str):
                raise SpecError(
                    f"{path}.headers key must be a string, got {_kind(key)}"
                )
            if not isinstance(value, str):
                raise SpecError(
                    f"{path}.headers.{key} must be a string, got {_kind(value)}"
                )
            headers[key] = value

    body_source = raw.get("body_source", BODY_SOURCE_NONE)
    if not isinstance(body_source, str):
        raise SpecError(
            f"{path}.body_source must be a string, got {_kind(body_source)}"
        )
    if body_source not in BODY_SOURCES:
        raise SpecError(
            f"{path}.body_source must be one of {list(BODY_SOURCES)}, "
            f"got {body_source!r}"
        )

    # ``body_path`` is a locator, so an empty string counts as missing (unlike
    # ``body``, where an empty string is a legitimate payload).
    body_path = _optional_str(raw, "body_path", path)
    body = _optional_str(raw, "body", path)
    if body_source == BODY_SOURCE_FILE and not body_path:
        raise SpecError(f"{path}.body_path is required when body_source is 'file'")
    if body_source == BODY_SOURCE_INLINE and body is None:
        body = ""

    success_status = _parse_success_check(raw.get("success_check"))

    return HttpConfig(
        method=method.upper(),
        url=url,
        headers=headers,
        body_source=body_source,
        body_path=body_path,
        body=body,
        success_status=success_status,
    )


def _parse_success_check(raw: Any) -> tuple[int, ...]:
    if raw is None:
        return DEFAULT_SUCCESS_STATUS
    path = "execution.config.success_check"
    if not isinstance(raw, dict):
        raise SpecError(f"{path} must be a mapping, got {_kind(raw)}")
    _reject_unknown_fields(raw, _SUCCESS_CHECK_FIELDS, path)

    if "http_status" not in raw or raw["http_status"] is None:
        return DEFAULT_SUCCESS_STATUS

    status_path = f"{path}.http_status"
    value = raw["http_status"]
    if isinstance(value, bool):
        raise SpecError(f"{status_path} must be an integer or a list of integers")
    if isinstance(value, int):
        return (value,)
    if isinstance(value, list):
        if not value:
            raise SpecError(f"{status_path} must not be an empty list")
        codes: list[int] = []
        for index, item in enumerate(value):
            if isinstance(item, bool) or not isinstance(item, int):
                raise SpecError(
                    f"{status_path}[{index}] must be an integer, got {_kind(item)}"
                )
            codes.append(item)
        return tuple(codes)
    raise SpecError(
        f"{status_path} must be an integer or a list of integers, got {_kind(value)}"
    )


def _optional_config(raw: Mapping[str, Any], path: str) -> Mapping[str, Any]:
    if "config" not in raw or raw["config"] is None:
        return {}
    config = raw["config"]
    if not isinstance(config, dict):
        raise SpecError(f"{path}.config must be a mapping, got {_kind(config)}")
    return copy.deepcopy(config)


def _optional_timeout(document: Mapping[str, Any]) -> int:
    if "timeout_seconds" not in document or document["timeout_seconds"] is None:
        return DEFAULT_TIMEOUT_SECONDS
    value = document["timeout_seconds"]
    path = f"{TOP}.timeout_seconds"
    if isinstance(value, bool) or not isinstance(value, int):
        raise SpecError(f"{path} must be an integer, got {_kind(value)}")
    if value <= 0:
        raise SpecError(f"{path} must be > 0")
    return value


def _require_str(
    mapping: Mapping[str, Any],
    key: str,
    path: str,
    *,
    max_length: int | None,
) -> str:
    field_path = f"{path}.{key}"
    if key not in mapping or mapping[key] is None:
        raise SpecError(f"{field_path} is required")
    value = mapping[key]
    if not isinstance(value, str):
        raise SpecError(f"{field_path} must be a string, got {_kind(value)}")
    if not value:
        raise SpecError(f"{field_path} must not be empty")
    if max_length is not None and len(value) > max_length:
        raise SpecError(f"{field_path} must be at most {max_length} characters")
    return value


def _optional_str(mapping: Mapping[str, Any], key: str, path: str) -> str | None:
    """Optional string field; unlike :func:`_require_str` an empty value is kept.

    ``description`` and an inline ``body`` may legitimately be ``""`` (all four
    languages treat those as present-but-empty); fields that must be non-empty
    go through :func:`_require_str` or get an explicit check.
    """
    if key not in mapping or mapping[key] is None:
        return None
    value = mapping[key]
    if not isinstance(value, str):
        raise SpecError(f"{path}.{key} must be a string, got {_kind(value)}")
    return value


def _reject_unknown_fields(
    mapping: Mapping[str, Any], allowed: frozenset[str], path: str
) -> None:
    for key in mapping:
        if not isinstance(key, str) or key not in allowed:
            raise SpecError(f"{path}.{key} is not allowed")


def _kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, dict):
        return "mapping"
    if isinstance(value, list):
        return "list"
    return type(value).__name__
