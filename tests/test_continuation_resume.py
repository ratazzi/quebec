from __future__ import annotations

import json

import pytest
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


def _fetch(qc_with_sqlalchemy, sql: str) -> list:
    """Run a read-only query without colliding with a running dispatcher.
    `{p}` in `sql` stands for the table prefix."""
    sql = sql.format(p=qc_with_sqlalchemy["prefix"])
    db_url = qc_with_sqlalchemy["db_url"]
    if db_url.startswith("sqlite:"):
        db_path = db_url.removeprefix("sqlite:///").split("?", 1)[0]

        def read() -> list:
            with readonly_connect(db_path) as observer:
                return observer.execute(sql).fetchall()

        return observe_sqlite(read)
    with qc_with_sqlalchemy["engine"].connect() as observer:
        return observer.execute(text(sql)).fetchall()


def _count(qc_with_sqlalchemy, table: str) -> int:
    return _fetch(qc_with_sqlalchemy, f"SELECT COUNT(*) FROM {{p}}_{table}")[0][0]


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

    wait_until(lambda: _count(qc_with_sqlalchemy, "ready_executions") == 1, timeout=5)
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


def _run_until_failed(qc_with_sqlalchemy, runs: int) -> None:
    """Perform the enqueued job, then each retry the dispatcher makes ready,
    `runs` times in all, and check the job ended up failed."""
    qc = qc_with_sqlalchemy["qc"]
    qc.drain_one().perform()
    qc.spawn_dispatcher()
    for _ in range(runs - 1):
        wait_until(
            lambda: _count(qc_with_sqlalchemy, "ready_executions") == 1, timeout=5
        )
        qc.drain_one().perform()

    assert _count(qc_with_sqlalchemy, "failed_executions") == 1
    assert _count(qc_with_sqlalchemy, "scheduled_executions") == 0
    assert _count(qc_with_sqlalchemy, "ready_executions") == 0
    assert _count(qc_with_sqlalchemy, "jobs") == runs


def test_retry_after_progress_fails_once_attempts_run_out(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    RetriesAfterProgressJob.first_step_runs = 0
    qc.register_job(RetriesAfterProgressJob)
    RetriesAfterProgressJob.perform_later(qc)

    # 3 attempts: the first run plus two retries, then the job fails.
    _run_until_failed(qc_with_sqlalchemy, runs=3)
    # Retries resumed after the completed step instead of starting over.
    assert RetriesAfterProgressJob.first_step_runs == 1


class ResumesThenRetriesJob(quebec.BaseClass, quebec.Continuable):
    """Advances its cursor and fails on every run: resumed once, then
    `retry_on` takes over."""

    max_resumptions = 1
    retry_on = [quebec.RetryStrategy((ValueError,), wait=0, attempts=2)]
    runs: list = []

    def perform(self) -> None:
        with self.step("process", start=0) as step:
            type(self).runs.append((self.executions, self.resumptions, step.cursor))
            step.advance()
            raise ValueError("keeps failing")


def test_retry_after_resumptions_keeps_the_resumption_count(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    ResumesThenRetriesJob.runs = []
    qc.register_job(ResumesThenRetriesJob)
    ResumesThenRetriesJob.perform_later(qc)

    _run_until_failed(qc_with_sqlalchemy, runs=3)

    # (executions, resumptions, cursor) seen by each run: resumed once (not
    # counted as an execution), then retried by retry_on with the resumption
    # count and cursor kept, then failed once its attempts ran out.
    assert ResumesThenRetriesJob.runs == [(0, 0, 0), (0, 1, 1), (1, 1, 2)]


class _WriteStepRetried(quebec.BaseClass, quebec.Continuable):
    """Completes the `write` step, then raises an error `retry_on` handles;
    subclasses differ in where that happens."""

    resume_errors_after_advancing = False
    retry_on = [quebec.RetryStrategy((ValueError,), wait=0, attempts=2)]
    write_runs = 0

    def write(self) -> None:
        with self.step("write") as step:
            if not step.skip:
                type(self).write_runs += 1


class FailsInBeforePerformJob(_WriteStepRetried):
    def before_perform(self) -> None:
        self.write()
        raise ValueError("raised by before_perform")

    def perform(self) -> None:
        pass


class FailsBeforeYieldJob(_WriteStepRetried):
    def around_perform(self):
        self.write()
        raise ValueError("raised by around_perform before yield")
        yield

    def perform(self) -> None:
        pass


class FailsAfterYieldJob(_WriteStepRetried):
    def around_perform(self):
        yield
        raise ValueError("raised by around_perform after perform")

    def perform(self) -> None:
        self.write()


@pytest.mark.parametrize(
    "job_class",
    [FailsInBeforePerformJob, FailsBeforeYieldJob, FailsAfterYieldJob],
    ids=["before_perform", "around_perform-before-yield", "around_perform-after-yield"],
)
def test_hook_error_keeps_step_progress(qc_with_sqlalchemy, job_class) -> None:
    qc = qc_with_sqlalchemy["qc"]
    job_class.write_runs = 0
    qc.register_job(job_class)
    job_class.perform_later(qc)

    _run_until_failed(qc_with_sqlalchemy, runs=2)
    # The retry skipped the step the first run completed.
    assert job_class.write_runs == 1


def test_resumed_job_does_not_grow_its_batch(qc_with_sqlalchemy) -> None:
    """Resuming is the same logical job, so it must not count towards the
    batch's `total_jobs` again."""
    qc = qc_with_sqlalchemy["qc"]
    ResumesOnceJob.resumed_runs = 0
    qc.register_job(ResumesOnceJob)

    with qc.batch() as batch:
        ResumesOnceJob.perform_later(qc)
    assert batch.reload().total_jobs == 1

    qc.drain_one().perform()
    assert batch.reload().total_jobs == 1
    assert not batch.finished

    qc.spawn_dispatcher()
    wait_until(lambda: _count(qc_with_sqlalchemy, "ready_executions") == 1, timeout=5)
    qc.drain_one().perform()

    assert ResumesOnceJob.resumed_runs == 1
    batch.reload()
    assert batch.finished
    assert (batch.total_jobs, batch.completed_jobs, batch.failed_jobs) == (1, 1, 0)
