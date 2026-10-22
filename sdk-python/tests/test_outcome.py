"""Outcome / RunResult contract surface (§2, §3)."""

from __future__ import annotations

import dataclasses
import json

import pytest

from startup_task import Outcome, RunResult


def test_outcome_members_are_the_four_contract_values():
    assert [member.name for member in Outcome] == [
        "EXECUTED",
        "SKIPPED_ALREADY_DONE",
        "SKIPPED_LOCKED",
        "FAILED",
    ]
    assert [member.value for member in Outcome] == [
        "EXECUTED",
        "SKIPPED_ALREADY_DONE",
        "SKIPPED_LOCKED",
        "FAILED",
    ]


@pytest.mark.parametrize("member", list(Outcome))
def test_outcome_string_form_is_the_upper_case_constant_name(member):
    assert str(member) == member.name
    assert f"{member}" == member.name
    assert member.to_string() == member.name
    assert member.value.isupper()
    assert member.value == member.value.upper()
    assert json.dumps(member) == json.dumps(member.name)


def test_outcome_lookup_by_value():
    assert Outcome("EXECUTED") is Outcome.EXECUTED
    assert Outcome("SKIPPED_LOCKED") is Outcome.SKIPPED_LOCKED
    with pytest.raises(ValueError):
        Outcome("executed")


def test_run_result_fields_and_defaults():
    result = RunResult(
        task_id="t", version="v", outcome=Outcome.FAILED, error="boom", duration_ms=12
    )
    assert result.task_id == "t"
    assert result.version == "v"
    assert result.outcome is Outcome.FAILED
    assert result.error == "boom"
    assert result.duration_ms == 12

    minimal = RunResult(task_id="t", version="v", outcome=Outcome.EXECUTED)
    assert minimal.error is None
    assert minimal.duration_ms == 0


def test_run_result_is_immutable():
    result = RunResult(task_id="t", version="v", outcome=Outcome.EXECUTED)
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.outcome = Outcome.FAILED  # type: ignore[misc]
