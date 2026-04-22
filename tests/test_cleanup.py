from __future__ import annotations

import quebec


class FinishedCleanupJob(quebec.BaseClass):
    def perform(self, value: int) -> None:
        return None


def test_count_clearable_jobs_and_clear_finished_jobs(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(FinishedCleanupJob)

    enqueued = FinishedCleanupJob.perform_later(qc, 10)
    execution = qc.drain_one()
    execution.perform()

    assert db_assert.count_jobs() == 1
    assert qc.count_clearable_jobs(finished_before=4102444800.0) == 1

    deleted = qc.clear_finished_jobs(finished_before=4102444800.0)

    assert deleted == 1
    assert db_assert.count_jobs() == 0
    assert qc.count_clearable_jobs(finished_before=4102444800.0) == 0
    assert enqueued.id is not None
