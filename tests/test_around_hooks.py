"""Tests for around_enqueue and around_perform (generator/yield style)."""

from __future__ import annotations

import quebec


# around_enqueue: yield to enqueue, code after yield runs post-enqueue
class AroundEnqueueJob(quebec.BaseClass):
    calls: list[str] = []

    def around_enqueue(self):
        type(self).calls.append("around:before")
        yield
        type(self).calls.append("around:after")

    def perform(self, value: int) -> None:
        return None


# around_enqueue: no yield = skip enqueue
class SkipEnqueueJob(quebec.BaseClass):
    calls: list[str] = []

    def around_enqueue(self):
        type(self).calls.append("skipped")
        # Must be a generator (contain yield somewhere) but not actually yield
        if False:
            yield

    def perform(self, value: int) -> None:
        return None


# around_perform: yield to perform
class AroundPerformJob(quebec.BaseClass):
    calls: list[str] = []

    def around_perform(self):
        type(self).calls.append("around:before")
        yield
        type(self).calls.append("around:after")

    def perform(self, value: int) -> None:
        type(self).calls.append(f"perform:{value}")


# around_perform: no yield = skip perform
class SkipPerformJob(quebec.BaseClass):
    calls: list[str] = []

    def around_perform(self):
        type(self).calls.append("skipped")
        if False:
            yield

    def perform(self, value: int) -> None:
        type(self).calls.append("should_not_run")


def test_around_enqueue_wraps_enqueue(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    AroundEnqueueJob.calls = []
    qc.register_job(AroundEnqueueJob)

    enqueued = AroundEnqueueJob.perform_later(qc, 1)

    assert AroundEnqueueJob.calls == ["around:before", "around:after"]
    assert enqueued.id is not None
    assert db_assert.count_jobs() == 1


def test_around_enqueue_no_yield_skips_enqueue(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    SkipEnqueueJob.calls = []
    qc.register_job(SkipEnqueueJob)

    enqueued = SkipEnqueueJob.perform_later(qc, 1)

    assert SkipEnqueueJob.calls == ["skipped"]
    assert enqueued.id is None
    assert db_assert.count_jobs() == 0


def test_around_perform_wraps_perform(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    AroundPerformJob.calls = []
    qc.register_job(AroundPerformJob)

    AroundPerformJob.perform_later(qc, 5)
    execution = qc.drain_one()
    execution.perform()

    assert AroundPerformJob.calls == ["around:before", "perform:5", "around:after"]
    assert db_assert.count_failed_executions() == 0


def test_around_perform_no_yield_skips_perform(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    SkipPerformJob.calls = []
    qc.register_job(SkipPerformJob)

    SkipPerformJob.perform_later(qc, 1)
    execution = qc.drain_one()
    execution.perform()

    assert SkipPerformJob.calls == ["skipped"]
    assert "should_not_run" not in SkipPerformJob.calls
