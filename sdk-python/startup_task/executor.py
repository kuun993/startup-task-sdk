"""Executors: the built-in ``http`` executor and the ``custom`` handler registry."""

from __future__ import annotations

import copy
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Mapping

import requests

from .spec import BODY_SOURCE_FILE, BODY_SOURCE_INLINE, TaskSpec

logger = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], None]

ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class ExecutionError(Exception):
    """A task execution failed; ``str(error)`` is reported as ``RunResult.error``."""


class HandlerNotRegisteredError(ExecutionError):
    pass


class MissingEnvironmentVariableError(ExecutionError):
    pass


def substitute_env(text: str) -> str:
    """Replace every ``${NAME}`` with ``os.environ[NAME]``.

    Missing variables raise :class:`MissingEnvironmentVariableError` with the
    contract's fixed message (``environment variable "NAME" is not set``).
    """

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        value = os.environ.get(name)
        if value is None:
            raise MissingEnvironmentVariableError(
                f'environment variable "{name}" is not set'
            )
        return value

    return ENV_PATTERN.sub(_replace, text)


class HandlerRegistry:
    """Name -> handler mapping; re-registering a name is an error."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, name: str, fn: Handler) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("handler name must be a non-empty string")
        if not callable(fn):
            raise ValueError(f'handler "{name}" must be callable')
        if name in self._handlers:
            raise ValueError(f'handler "{name}" is already registered')
        self._handlers[name] = fn

    def get(self, name: str) -> Handler:
        try:
            return self._handlers[name]
        except KeyError:
            raise HandlerNotRegisteredError(
                f'handler "{name}" is not registered'
            ) from None

    def contains(self, name: str) -> bool:
        return name in self._handlers

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._handlers)


def execute(spec: TaskSpec, registry: HandlerRegistry) -> None:
    """Run ``spec``'s executor; raises :class:`ExecutionError` on failure."""
    execution = spec.execution
    if execution.executor == "custom":
        run_custom(execution.handler or "", execution.config, registry)
        return
    run_http(spec)


def run_custom(handler_name: str, config: Mapping[str, Any], registry: HandlerRegistry) -> None:
    handler = registry.get(handler_name)
    payload = copy.deepcopy(dict(config))
    handler(payload)


def run_http(spec: TaskSpec) -> None:
    http = spec.execution.http
    if http is None:  # pragma: no cover - guarded by the spec validator
        raise ExecutionError("execution.config is missing")

    url = substitute_env(http.url)
    headers = {key: substitute_env(value) for key, value in http.headers.items()}
    body = _body_bytes(spec, http.body_source, http.body_path, http.body)

    logger.debug("http request %s %s", http.method, url)
    try:
        response = requests.request(
            http.method,
            url,
            headers=headers or None,
            data=body,
            timeout=spec.timeout_seconds,
        )
    except requests.RequestException as exc:
        raise ExecutionError(f"http request failed: {exc}") from exc

    if response.status_code not in http.success_status:
        raise ExecutionError(
            f"http status {response.status_code} not in {http.success_status_text}"
            f" (body: {_response_text(response)})"
        )


def _body_bytes(
    spec: TaskSpec,
    body_source: str,
    body_path: str | None,
    body: str | None,
) -> bytes | None:
    if body_source == BODY_SOURCE_FILE:
        path = Path(body_path or "")
        if not path.is_absolute():
            path = spec.base_dir / path
        try:
            return path.read_bytes()
        except OSError as exc:
            raise ExecutionError(f"cannot read body file {path}: {exc}") from exc
    if body_source == BODY_SOURCE_INLINE:
        return substitute_env(body or "").encode("utf-8")
    return None


def _response_text(response: requests.Response) -> str:
    try:
        return response.text
    except Exception:  # noqa: BLE001 - decoding must never mask the failure
        return response.content.decode("utf-8", "replace")


__all__ = [
    "ENV_PATTERN",
    "ExecutionError",
    "Handler",
    "HandlerNotRegisteredError",
    "HandlerRegistry",
    "MissingEnvironmentVariableError",
    "execute",
    "run_custom",
    "run_http",
    "substitute_env",
]
