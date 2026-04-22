"""Tests for perform-side lifecycle hooks: before_perform, after_perform, around_perform."""

from __future__ import annotations

import quebec


class BeforePerformJob(quebec.BaseClass):
    calls: list[str] = []

    def before_perform(self) -> None:
        type(self).calls.append("before_perform")

    def perform(self, value: int) -> None:
        type(self).calls.append(f"perform:{value}")


class AfterPerformJob(quebec.BaseClass):
    calls: list[str] = []

    def after_perform(self) -> None:
        type(self).calls.append("after_perform")

    def perform(self, value: int) -> None:
        type(self).calls.append(f"perform:{value}")


class BeforeAfterPerformJob(quebec.BaseClass):
    calls: list[str] = []

    def before_perform(self) -> None:
        type(self).calls.append("before")

    def after_perform(self) -> None:
        type(self).calls.append("after")

    def perform(self, value: int) -> None:
        type(self).calls.append(f"perform:{value}")


class AfterPerformOnFailureJob(quebec.BaseClass):
    calls: list[str] = []

    def after_perform(self) -> None:
        type(self).calls.append("after_perform")

    def perform(self, value: int) -> None:
        raise ValueError("boom")


def test_before_perform_runs_before_perform(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    BeforePerformJob.calls = []
    qc.register_job(BeforePerformJob)

    BeforePerformJob.perform_later(qc, 42)
    execution = qc.drain_one()
    execution.perform()

    assert BeforePerformJob.calls == ["before_perform", "perform:42"]
    assert db_assert.count_failed_executions() == 0


def test_after_perform_runs_after_perform(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    AfterPerformJob.calls = []
    qc.register_job(AfterPerformJob)

    AfterPerformJob.perform_later(qc, 7)
    execution = qc.drain_one()
    execution.perform()

    assert AfterPerformJob.calls == ["perform:7", "after_perform"]
    assert db_assert.count_failed_executions() == 0


def test_before_and_after_perform_ordering(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    BeforeAfterPerformJob.calls = []
    qc.register_job(BeforeAfterPerformJob)

    BeforeAfterPerformJob.perform_later(qc, 99)
    execution = qc.drain_one()
    execution.perform()

    assert BeforeAfterPerformJob.calls == ["before", "perform:99", "after"]


def test_after_perform_not_called_on_failure(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    AfterPerformOnFailureJob.calls = []
    qc.register_job(AfterPerformOnFailureJob)

    AfterPerformOnFailureJob.perform_later(qc, 1)
    execution = qc.drain_one()
    execution.perform()

    assert AfterPerformOnFailureJob.calls == []
    assert db_assert.count_failed_executions() == 1
