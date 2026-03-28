from __future__ import annotations

import quebec

from .helpers import get_job_by_active_job_id


class ExecutingJob(quebec.BaseClass):
    executions: list[tuple[str, str]] = []

    def perform(self, subject: str, **kwargs) -> None:
        type(self).executions.append((subject, kwargs["status"]))


def test_pick_job_and_perform_finishes_job(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    ExecutingJob.executions = []
    qc.register_job(ExecutingJob)

    enqueued = ExecutingJob.perform_later(qc, "invoice-123", status="queued")
    execution = qc.drain_one()

    assert execution.jid == enqueued.active_job_id
    assert execution.queue == "default"
    assert execution.class_name == "ExecutingJob"

    execution.perform()
    session.expire_all()

    persisted_job = get_job_by_active_job_id(session, prefix, enqueued.active_job_id)

    assert ExecutingJob.executions == [("invoice-123", "queued")]
    assert persisted_job["finished_at"] is not None
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 0
