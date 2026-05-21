"""Tests for the failed-job retry/discard Python API on the Quebec instance."""

from __future__ import annotations

import quebec
from sqlalchemy import text


class FailingJob(quebec.ActiveJob):
    queue_as = "default"

    def perform(self, *args, **kwargs):
        raise ValueError("boom")


class OtherFailingJob(quebec.ActiveJob):
    queue_as = "default"

    def perform(self, *args, **kwargs):
        raise RuntimeError("nope")


class ConcurrencyFailingJob(quebec.ActiveJob):
    queue_as = "default"
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "exclusive-resource"

    def perform(self, *args, **kwargs):
        raise ValueError("boom")


def _enqueue_and_fail(qc, job_cls):
    """Enqueue a job and run it to completion so it lands in failed_executions."""
    enqueued = job_cls.perform_later(qc)
    execution = qc.drain_one()
    execution.perform()
    return enqueued


def _count(session, prefix, table, where=""):
    sql = f"SELECT COUNT(*) AS c FROM {prefix}_{table} {where}".strip()
    return session.execute(text(sql)).scalar()


def test_retry_failed_moves_failed_execution_to_ready(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)
    job = _enqueue_and_fail(qc, FailingJob)
    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 1
    assert _count(session, prefix, "ready_executions") == 0

    assert qc.retry_failed(job.id) is True

    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 0
    assert (
        _count(
            session,
            prefix,
            "ready_executions",
            where=f"WHERE job_id = {job.id}",
        )
        == 1
    )


def test_retry_failed_respects_held_semaphore(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrencyFailingJob)

    failed = _enqueue_and_fail(qc, ConcurrencyFailingJob)
    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 1
    assert _count(session, prefix, "ready_executions") == 0

    blocker = ConcurrencyFailingJob.perform_later(qc)
    session.expire_all()
    assert _count(session, prefix, "ready_executions") == 1

    assert qc.retry_failed(failed.id) is True

    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 0
    assert _count(session, prefix, "ready_executions") == 1
    ready_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_ready_executions")
    ).scalar()
    assert ready_job_id == blocker.id

    assert _count(session, prefix, "blocked_executions") == 1
    blocked_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions")
    ).scalar()
    assert blocked_job_id == failed.id


def test_retry_failed_unknown_id_returns_false(qc) -> None:
    assert qc.retry_failed(999_999) is False


def test_discard_failed_marks_finished_and_removes_execution(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)
    job = _enqueue_and_fail(qc, FailingJob)
    session.expire_all()

    assert qc.discard_failed(job.id) is True

    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 0
    finished_at = session.execute(
        text(f"SELECT finished_at FROM {prefix}_jobs WHERE id = :id"),
        {"id": job.id},
    ).scalar()
    assert finished_at is not None


def test_discard_failed_unknown_id_returns_false(qc) -> None:
    assert qc.discard_failed(999_999) is False


def test_retry_all_failed_without_filters_retries_everything(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)
    qc.register_job(OtherFailingJob)
    _enqueue_and_fail(qc, FailingJob)
    _enqueue_and_fail(qc, OtherFailingJob)
    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 2

    count = qc.retry_all_failed()
    assert count == 2

    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 0
    assert _count(session, prefix, "ready_executions") == 2


def test_retry_all_failed_respects_held_semaphore(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrencyFailingJob)

    failed = _enqueue_and_fail(qc, ConcurrencyFailingJob)
    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 1
    assert _count(session, prefix, "ready_executions") == 0

    blocker = ConcurrencyFailingJob.perform_later(qc)
    session.expire_all()
    assert _count(session, prefix, "ready_executions") == 1

    count = qc.retry_all_failed()
    assert count == 1

    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 0
    assert _count(session, prefix, "ready_executions") == 1
    ready_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_ready_executions")
    ).scalar()
    assert ready_job_id == blocker.id

    assert _count(session, prefix, "blocked_executions") == 1
    blocked_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions")
    ).scalar()
    assert blocked_job_id == failed.id


def test_retry_all_failed_filters_by_class_name(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)
    qc.register_job(OtherFailingJob)
    _enqueue_and_fail(qc, FailingJob)
    _enqueue_and_fail(qc, OtherFailingJob)
    session.expire_all()

    count = qc.retry_all_failed(class_name=FailingJob.__qualname__)
    assert count == 1

    session.expire_all()
    # The OtherFailingJob row remains failed; the FailingJob row is back in
    # ready_executions.
    assert _count(session, prefix, "failed_executions") == 1
    assert _count(session, prefix, "ready_executions") == 1


def test_discard_all_failed_clears_everything(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)
    _enqueue_and_fail(qc, FailingJob)
    _enqueue_and_fail(qc, FailingJob)
    session.expire_all()

    count = qc.discard_all_failed()
    assert count == 2

    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 0
    assert _count(session, prefix, "ready_executions") == 0
    assert (
        _count(
            session,
            prefix,
            "jobs",
            where="WHERE finished_at IS NOT NULL",
        )
        == 2
    )


def test_retry_all_failed_rejects_nan_timestamp(qc) -> None:
    import math

    try:
        qc.retry_all_failed(since=math.nan)
    except ValueError as exc:
        assert "since" in str(exc)
    else:
        raise AssertionError("expected ValueError for NaN timestamp")


def test_discard_all_failed_filters_by_class_name(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)
    qc.register_job(OtherFailingJob)
    failing_job = _enqueue_and_fail(qc, FailingJob)
    _enqueue_and_fail(qc, OtherFailingJob)
    session.expire_all()

    count = qc.discard_all_failed(class_name=FailingJob.__qualname__)
    assert count == 1

    session.expire_all()
    # Only OtherFailingJob's failed_execution remains; FailingJob's row is
    # gone and the job itself is finished.
    assert _count(session, prefix, "failed_executions") == 1
    finished_at = session.execute(
        text(f"SELECT finished_at FROM {prefix}_jobs WHERE id = :id"),
        {"id": failing_job.id},
    ).scalar()
    assert finished_at is not None


def test_retry_all_failed_filters_by_queue_name(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class EmailFailingJob(quebec.ActiveJob):
        queue_as = "emails"

        def perform(self, *args, **kwargs):
            raise ValueError("boom")

    qc.register_job(FailingJob)
    qc.register_job(EmailFailingJob)
    _enqueue_and_fail(qc, FailingJob)
    _enqueue_and_fail(qc, EmailFailingJob)
    session.expire_all()

    count = qc.retry_all_failed(queue_name="emails")
    assert count == 1

    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 1
    assert (
        _count(
            session,
            prefix,
            "ready_executions",
            where="WHERE queue_name = 'emails'",
        )
        == 1
    )


def test_retry_all_failed_filters_by_error_like(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)  # raises ValueError
    qc.register_job(OtherFailingJob)  # raises RuntimeError
    _enqueue_and_fail(qc, FailingJob)
    _enqueue_and_fail(qc, OtherFailingJob)
    session.expire_all()

    # Error blob is JSON; class name appears in the message body.
    count = qc.retry_all_failed(error_like="%RuntimeError%")
    assert count == 1

    session.expire_all()
    # The ValueError-failed row remains, the RuntimeError-failed row is back
    # in ready_executions.
    assert _count(session, prefix, "failed_executions") == 1
    assert _count(session, prefix, "ready_executions") == 1


def test_retry_all_failed_filters_by_since_until(qc_with_sqlalchemy) -> None:
    """``since`` / ``until`` filter on ``failed_executions.created_at``."""
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(FailingJob)
    job = _enqueue_and_fail(qc, FailingJob)
    session.expire_all()

    # Read back the exact created_at the DB stored so we can build a window
    # around it without flaky clock-skew issues.
    created_at = session.execute(
        text(f"SELECT created_at FROM {prefix}_failed_executions WHERE job_id = :id"),
        {"id": job.id},
    ).scalar()
    # SQLite returns ISO 8601; parse to epoch seconds.
    import datetime

    parsed = datetime.datetime.fromisoformat(str(created_at).replace(" ", "T"))
    ts = parsed.replace(tzinfo=datetime.timezone.utc).timestamp()

    # Window that strictly precedes the row → no match.
    assert qc.retry_all_failed(until=ts - 60) == 0
    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 1

    # Window that covers the row → matches.
    assert qc.retry_all_failed(since=ts - 60, until=ts + 60) == 1
    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 0


def test_parse_timestamp_handles_negative_fractional(qc_with_sqlalchemy) -> None:
    """Regression: -0.5 must round toward -inf, not toward 0.

    The bug was: ``ts as i64`` truncates toward zero, and ``ts - secs as f64``
    becomes negative which saturates to 0 when cast to ``u32``. So ``-0.5``
    landed at the epoch instead of 0.5 s before it.

    A pure end-to-end discriminator: seed a failed_executions row whose
    ``created_at`` is between the buggy bound (epoch) and the fixed bound
    (1969-12-31T23:59:59.500). Then ``until=-0.5`` must NOT match the row
    (fixed) — but it WOULD have matched under the bug (since the row sits
    before epoch).
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    # Job + failed_execution at exactly 1969-12-31 23:59:59.750: pre-epoch
    # but AFTER the fixed -0.5 bound (23:59:59.500). Comparison is on
    # failed_executions.created_at, which the find_all query bounds via
    # `>= since` / `<= until`.
    session.execute(
        text(
            f"INSERT INTO {prefix}_jobs "
            f"(queue_name, class_name, arguments, priority, created_at, updated_at) "
            f"VALUES ('default', 'X', '[]', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        )
    )
    job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.execute(
        text(
            f"INSERT INTO {prefix}_failed_executions "
            f"(job_id, error, created_at) VALUES (:jid, '{{}}', :ts)"
        ),
        {"jid": job_id, "ts": "1969-12-31 23:59:59.750"},
    )
    session.commit()

    # Buggy parse_optional_timestamp: until=-0.5 → epoch. Row is BEFORE
    # epoch → row.created_at <= bound → match → returns 1, row deleted.
    # Fixed: until=-0.5 → 23:59:59.500. Row at 23:59:59.750 is AFTER bound
    # → no match → returns 0, row stays.
    assert qc.retry_all_failed(until=-0.5) == 0
    session.expire_all()
    assert _count(session, prefix, "failed_executions") == 1
