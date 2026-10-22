"""Runnable example: SQLite store + both executors via ``run_all``.

    python examples/run_examples.py

The config-center task is expected to end in ``FAILED`` on a development machine
(nothing is listening on 127.0.0.1:8080); that failure is reported, not raised.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path

try:
    import startup_task  # noqa: F401
except ModuleNotFoundError:  # running from a checkout without `pip install -e .`
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from startup_task import (  # noqa: E402
    Coordinator,
    Outcome,
    RunResult,
    SqliteStore,
    ensure_schema,
    load_spec,
)

EXAMPLE_DIR = Path(__file__).resolve().parent


def warm_up_cache(config: dict) -> None:
    """Stand-in for a real cache warm-up; ``config`` is ``execution.config``."""
    keys = config.get("keys", [])
    ttl_seconds = config.get("ttl_seconds", 600)
    if not isinstance(keys, list) or not keys:
        raise ValueError(f"keys must be a non-empty list, got {keys!r}")
    if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        raise ValueError(f"invalid ttl_seconds: {ttl_seconds!r}")

    print(f"  [handler] warm_up_cache ttl_seconds={ttl_seconds}")
    for key in keys:
        print(f"  [handler] warmed {key} (ttl={ttl_seconds}s)")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    os.environ.setdefault("CONFIG_CENTER_URL", "http://127.0.0.1:8080")
    os.environ.setdefault("CONFIG_CENTER_TOKEN", "example-token")
    os.environ.setdefault("APP_NAME", "demo-app")

    workdir = Path(tempfile.mkdtemp(prefix="startup-task-example-"))
    db_path = workdir / "startup_tasks.db"

    store = SqliteStore(db_path)
    ensure_schema(store)
    print(f"sqlite store: {db_path}")

    coordinator = Coordinator(store, owner_id="examples/local")
    coordinator.register_handler("warm_up_cache", warm_up_cache)

    specs = [
        load_spec(EXAMPLE_DIR / "publish_app_settings.yaml"),
        load_spec(EXAMPLE_DIR / "warm_up_cache.yaml"),
    ]

    print("\nfirst run:")
    results = coordinator.run_all(specs)
    _print_results(results)

    print("\nsecond run (same version -> already done):")
    _print_results(coordinator.run_all(specs))

    print("\nrun again with a bumped version via the bump helper:")
    bumped = load_spec(EXAMPLE_DIR / "warm_up_cache.yaml")
    bumped = type(bumped)(
        task_id=bumped.task_id,
        version=bumped.version + "-b",
        execution=bumped.execution,
        description=bumped.description,
        timeout_seconds=bumped.timeout_seconds,
        base_dir=bumped.base_dir,
    )
    _print_results([coordinator.run(bumped)])

    failed = [r for r in results if r.outcome is Outcome.FAILED]
    if failed:
        print(
            "\nnote: the config-center task failed as expected on this machine "
            f"({failed[0].error})"
        )
    return 0


def _print_results(results: list[RunResult]) -> None:
    for result in results:
        error = f" error={result.error}" if result.error else ""
        print(
            f"  {result.task_id}@{result.version} -> "
            f"{result.outcome} ({result.duration_ms} ms){error}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
