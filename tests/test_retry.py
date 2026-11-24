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


def _retry_delay_seconds(qc_with_sqlalchemy, job_class) -> float:
    """Perform one failing attempt and return the retry's scheduled delay."""
    from datetime import datetime, timezone

    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(job_class)
    enqueued = job_class.perform_later(qc, 1)
    before = datetime.now(timezone.utc).replace(tzinfo=None)
    qc.drain_one().perform()
    session.expire_all()

    scheduled_at = session.execute(
        text(
            f"SELECT scheduled_at FROM {prefix}_jobs "
            f"WHERE active_job_id = :id ORDER BY id DESC LIMIT 1"
        ),
        {"id": enqueued.active_job_id},
    ).scalar_one()
    if isinstance(scheduled_at, str):
        scheduled_at = datetime.fromisoformat(scheduled_at)
    return (scheduled_at.replace(tzinfo=None) - before).total_seconds()


class PolynomialJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,), wait="polynomially_longer", attempts=3, jitter=0
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError("boom")


class CustomBackoffJob(quebec.BaseClass):
    seen = []

    @staticmethod
    def _backoff(executions: int) -> timedelta:
        CustomBackoffJob.seen.append(executions)
        return timedelta(seconds=100 * executions)

    retry_on = [quebec.RetryStrategy((ValueError,), wait=_backoff, attempts=3)]

    def perform(self, value: int) -> None:
        raise ValueError("boom")


def test_polynomially_longer_first_retry(qc_with_sqlalchemy) -> None:
    # ActiveJob: executions**4 + 2 with executions=1 -> 3s
    delay = _retry_delay_seconds(qc_with_sqlalchemy, PolynomialJob)
    assert 2 <= delay <= 5


def test_custom_backoff_callable_receives_executions(qc_with_sqlalchemy) -> None:
    CustomBackoffJob.seen.clear()
    delay = _retry_delay_seconds(qc_with_sqlalchemy, CustomBackoffJob)
    assert CustomBackoffJob.seen == [1]
    assert 99 <= delay <= 102


def test_fixed_wait_and_default_jitter() -> None:
    strategy = quebec.RetryStrategy((ValueError,), wait=10)
    assert strategy.wait == timedelta(seconds=10)
    assert strategy.jitter == 0.15


def test_retry_strategy_rejects_bad_wait() -> None:
    import pytest

    with pytest.raises(ValueError):
        quebec.RetryStrategy((ValueError,), wait="exponentially_longer")
    with pytest.raises(ValueError):
        quebec.RetryStrategy((ValueError,), wait=-1)
    with pytest.raises(ValueError):
        quebec.RetryStrategy((ValueError,), wait=timedelta(seconds=-1))
    with pytest.raises(ValueError):
        quebec.RetryStrategy((ValueError,), wait=1e30)
    with pytest.raises(TypeError):
        quebec.RetryStrategy((ValueError,), wait=object())
    with pytest.raises(ValueError):
        quebec.RetryStrategy((ValueError,), jitter=-0.1)


def _raising_backoff(executions: int) -> float:
    raise KeyError(f"backoff {executions}")


class RaisingBackoffJob(quebec.BaseClass):
    retry_on = [quebec.RetryStrategy((ValueError,), wait=_raising_backoff, attempts=3)]

    def perform(self, value: int) -> None:
        raise ValueError("boom")


class HugeBackoffJob(quebec.BaseClass):
    retry_on = [quebec.RetryStrategy((ValueError,), wait=lambda n: 1e30, attempts=3)]

    def perform(self, value: int) -> None:
        raise ValueError("boom")


def _failed_error(qc_with_sqlalchemy) -> dict:
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]
    session.expire_all()
    row = session.execute(
        text(f"SELECT error FROM {prefix}_failed_executions")
    ).scalar_one()
    return json.loads(row)


def test_raising_custom_wait_fails_job_with_its_error(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(RaisingBackoffJob)

    enqueued = RaisingBackoffJob.perform_later(qc, 1)
    qc.drain_one().perform()

    assert db_assert.count_failed_executions() == 1
    assert db_assert.count_scheduled_executions() == 0
    error = _failed_error(qc_with_sqlalchemy)
    assert error["exception_class"] == "KeyError"
    assert "backoff 1" in error["message"]
    assert any("_raising_backoff" in line for line in error["backtrace"])
    payload = json.loads(db_assert.get_job(enqueued.id)["arguments"])
    assert payload["exception_executions"] == {"[ValueError]": 1}


def test_out_of_range_custom_wait_fails_job_instead_of_panicking(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(HugeBackoffJob)

    HugeBackoffJob.perform_later(qc, 1)
    qc.drain_one().perform()

    assert db_assert.count_failed_executions() == 1
    assert db_assert.count_scheduled_executions() == 0
    assert _failed_error(qc_with_sqlalchemy)["exception_class"] == "ValueError"
