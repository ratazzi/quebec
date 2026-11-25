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


def _ready_count(qc_with_sqlalchemy) -> int:
    """Count ready executions without colliding with a running dispatcher."""
    prefix = qc_with_sqlalchemy["prefix"]
    db_url = qc_with_sqlalchemy["db_url"]
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
    ResumesOnceJob.resumed_runs = 0
    qc.register_job(ResumesOnceJob)

    ResumesOnceJob.perform_later(qc)
    qc.drain_one().perform()
    qc.spawn_dispatcher()

    wait_until(lambda: _ready_count(qc_with_sqlalchemy) == 1, timeout=5)
    qc.drain_one().perform()

    assert ResumesOnceJob.resumed_runs == 1
    assert db_assert.count_jobs() == 2
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_scheduled_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 0


class RetriesAfterProgressJob(quebec.BaseClass, quebec.Continuable):
    """Completes one step, then fails in the next with an error `retry_on`
    handles. Errors go to `retry_on` instead of being resumed."""

    resume_errors_after_advancing = False
    retry_on = [quebec.RetryStrategy((ValueError,), wait=0, attempts=3)]
    first_step_runs = 0

    def perform(self) -> None:
        with self.step("first") as step:
            if not step.skip:
                type(self).first_step_runs += 1
        with self.step("second"):
            raise ValueError("second step keeps failing")


class RetriesAfterResumeLimitJob(RetriesAfterProgressJob):
    """Default `resume_errors_after_advancing`, but no resumptions left, so
    the error reaches `retry_on`."""

    resume_errors_after_advancing = True
    max_resumptions = 0


def _job_rows(session, prefix, active_job_id):
    session.expire_all()
    return session.execute(
        text(
            f"SELECT id, arguments FROM {prefix}_jobs "
            f"WHERE active_job_id = :active_job_id ORDER BY id"
        ),
        {"active_job_id": active_job_id},
    ).fetchall()


def test_retry_after_progress_keeps_steps_and_counts_the_attempt(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    for job_class in (RetriesAfterProgressJob, RetriesAfterResumeLimitJob):
        qc.register_job(job_class)
        enqueued = job_class.perform_later(qc)
        qc.drain_one().perform()

        rows = _job_rows(
            qc_with_sqlalchemy["session"],
            qc_with_sqlalchemy["prefix"],
            enqueued.active_job_id,
        )
        assert len(rows) == 2, job_class.__name__
        retry = json.loads(rows[1].arguments)
        # The attempt is counted, so retry_on can run out of attempts...
        assert retry["executions"] == 1, job_class.__name__
        # ...and the retry resumes after the completed step.
        assert retry["continuation"]["completed"] == ["first"], job_class.__name__
        assert retry["resumptions"] == 0, job_class.__name__


def test_retry_after_progress_fails_once_attempts_run_out(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    RetriesAfterProgressJob.first_step_runs = 0
    qc.register_job(RetriesAfterProgressJob)

    RetriesAfterProgressJob.perform_later(qc)
    qc.drain_one().perform()
    qc.spawn_dispatcher()

    # 3 attempts: the first run plus two retries, then the job fails.
    for _ in range(2):
        wait_until(lambda: _ready_count(qc_with_sqlalchemy) == 1, timeout=5)
        qc.drain_one().perform()

    assert db_assert.count_failed_executions() == 1
    assert db_assert.count_scheduled_executions() == 0
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_jobs() == 3
    # Retries resumed after the completed step instead of starting over.
    assert RetriesAfterProgressJob.first_step_runs == 1
