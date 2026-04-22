from __future__ import annotations

import json

import quebec

from .helpers import get_job_by_active_job_id


class FailingJob(quebec.BaseClass):
    def perform(self, value: int) -> None:
        raise ValueError(f"bad payload: {value}")


def test_pick_job_and_perform_records_failed_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)

    enqueued = FailingJob.perform_later(qc, 12)
    execution = qc.drain_one()
    execution.perform()
    session.expire_all()

    persisted_job = get_job_by_active_job_id(session, prefix, enqueued.active_job_id)
    payload = json.loads(persisted_job["arguments"])

    assert persisted_job["finished_at"] is None
    assert payload["executions"] == 1
    assert payload["exception_executions"]["[ValueError]"] == 1
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 1
    assert db_assert.job_is_failed(persisted_job["id"]) is True
