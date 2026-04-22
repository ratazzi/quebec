from __future__ import annotations

import json
from datetime import timedelta

import quebec
from sqlalchemy import text


class RetryingJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=30),
            attempts=3,
            handler=None,
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"retry me: {value}")


def test_pick_job_and_perform_schedules_retry(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(RetryingJob)

    enqueued = RetryingJob.perform_later(qc, 55)
    execution = qc.drain_one()
    execution.perform()
    session.expire_all()

    rows = session.execute(
        text(
            f"SELECT id, active_job_id, arguments, finished_at, scheduled_at "
            f"FROM {prefix}_jobs WHERE active_job_id = :active_job_id ORDER BY id"
        ),
        {"active_job_id": enqueued.active_job_id},
    ).fetchall()

    assert len(rows) == 2
    original, retried = rows
    original_payload = json.loads(original.arguments)
    retried_payload = json.loads(retried.arguments)

    assert original.finished_at is not None
    assert original_payload["executions"] == 0
    assert retried.scheduled_at is not None
    assert retried_payload["executions"] == 1
    assert retried_payload["exception_executions"]["[ValueError]"] == 1
    assert db_assert.count_jobs() == 2
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 0
    assert db_assert.count_scheduled_executions() == 1
