from __future__ import annotations

import json

import quebec
from sqlalchemy import text

from .helpers import observe_sqlite, readonly_connect, wait_until


class InterruptingContinuationJob(quebec.BaseClass, quebec.Continuable):
    def perform(self) -> None:
        with self.step("process", start=0) as step:
            step.advance(from_=0)
            raise quebec.JobInterrupted("pause for resume")


class ResumesOnceJob(quebec.BaseClass, quebec.Continuable):
    resumed_runs = 0

    def perform(self) -> None:
        with self.step("process", start=0) as step:
            if not step.resumed:
                step.advance(from_=0)
                raise quebec.JobInterrupted("resume once")
            type(self).resumed_runs += 1


def test_job_interrupted_reenqueues_with_continuation_state(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(InterruptingContinuationJob)

    enqueued = InterruptingContinuationJob.perform_later(qc)
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
    original, resumed = rows
    resumed_payload = json.loads(resumed.arguments)

    assert original.finished_at is not None
    assert resumed.scheduled_at is not None
    assert resumed_payload["resumptions"] == 1
    assert resumed_payload["continuation"]["current"][0] == "process"
    assert resumed_payload["continuation"]["current"][1] == 1
    assert db_assert.count_jobs() == 2
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 0
    assert db_assert.count_scheduled_executions() == 1


def test_interrupted_job_resumes_after_dispatch(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]
    prefix = qc_with_sqlalchemy["prefix"]
    db_url = qc_with_sqlalchemy["db_url"]
    ResumesOnceJob.resumed_runs = 0
    qc.register_job(ResumesOnceJob)

    ResumesOnceJob.perform_later(qc)
    qc.drain_one().perform()
    qc.spawn_dispatcher()

    def ready_count() -> int:
        if db_url.startswith("sqlite:"):
            db_path = db_url.removeprefix("sqlite:///").split("?", 1)[0]

            def read() -> int:
                with readonly_connect(db_path) as observer:
                    return observer.execute(
                        f"SELECT COUNT(*) FROM {prefix}_ready_executions"
                    ).fetchone()[0]

            return observe_sqlite(read)
        with qc_with_sqlalchemy["engine"].connect() as observer:
            return observer.execute(
                text(f"SELECT COUNT(*) FROM {prefix}_ready_executions")
            ).scalar_one()

    wait_until(lambda: ready_count() == 1, timeout=5)
    qc.drain_one().perform()

    assert ResumesOnceJob.resumed_runs == 1
    assert db_assert.count_jobs() == 2
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_scheduled_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 0
