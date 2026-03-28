"""Tests that perform_all_later does NOT trigger enqueue callbacks (matching ActiveJob behavior)."""

from __future__ import annotations

import quebec


class TrackedBulkJob(quebec.BaseClass):
    before_calls: int = 0
    after_calls: int = 0

    def before_enqueue(self) -> None:
        type(self).before_calls += 1

    def after_enqueue(self) -> None:
        type(self).after_calls += 1

    def perform(self, value: int) -> None:
        return None


def test_perform_all_later_does_not_trigger_enqueue_hooks(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    TrackedBulkJob.before_calls = 0
    TrackedBulkJob.after_calls = 0
    qc.register_job(TrackedBulkJob)

    jobs = [TrackedBulkJob.build(i) for i in range(5)]
    inserted = qc.perform_all_later(jobs)

    assert inserted == 5
    assert TrackedBulkJob.before_calls == 0
    assert TrackedBulkJob.after_calls == 0
    assert db_assert.count_jobs() == 5


def test_perform_later_does_trigger_enqueue_hooks(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Contrast: single perform_later DOES trigger hooks."""
    qc = qc_with_sqlalchemy["qc"]

    TrackedBulkJob.before_calls = 0
    TrackedBulkJob.after_calls = 0
    qc.register_job(TrackedBulkJob)

    TrackedBulkJob.perform_later(qc, 1)

    assert TrackedBulkJob.before_calls == 1
    assert TrackedBulkJob.after_calls == 1
