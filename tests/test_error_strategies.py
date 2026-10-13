from __future__ import annotations

import json
from datetime import timedelta

import quebec
from sqlalchemy import text


class RetryExhaustingJob(quebec.BaseClass):
    # Handlers receive (job, error), matching ActiveJob's `yield self, error`.
    handled: list[tuple[str, str]] = []
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=1),
            attempts=1,
            handler=lambda job, exc: RetryExhaustingJob.handled.append(
                (type(job).__name__, str(exc))
            ),
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"boom {value}")


class RetryWithAttemptsJob(quebec.BaseClass):
    handled: list[str] = []
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=1),
            attempts=3,
            handler=lambda job, exc: RetryWithAttemptsJob.handled.append(str(exc)),
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"boom {value}")


class MixedSpecJob(quebec.BaseClass):
    # A tuple mixing a real exception class with a non-class entry: the valid
    # class must still match (the bogus entry is ignored, with a warning).
    handled: list[str] = []
    retry_on = [
        quebec.RetryStrategy(
            (ValueError, lambda exc: exc),
            wait=timedelta(seconds=1),
            attempts=1,
            handler=lambda job, exc: MixedSpecJob.handled.append(str(exc)),
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"mixed {value}")


def _alert_and_reraise(job, exc):
    RetryReraisingJob.handled.append(str(exc))
    raise exc


class RetryReraisingJob(quebec.BaseClass):
    # Re-raising from the exhaustion handler keeps the failure, as in ActiveJob.
    handled: list[str] = []
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=1),
            attempts=1,
            handler=_alert_and_reraise,
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"reraise {value}")


def _raise_other(job, exc):
    raise RuntimeError(f"handler saw {exc}")


class RetryHandlerRaisesOtherJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=1),
            attempts=1,
            handler=_raise_other,
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"original {value}")


def _failed_error(qc_with_sqlalchemy) -> dict:
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]
    session.expire_all()
    row = session.execute(
        text(f"SELECT error FROM {prefix}_failed_executions")
    ).scalar_one()
    return json.loads(row)


class DiscardingJob(quebec.BaseClass):
    discard_events: list[str] = []
    discard_on = [quebec.DiscardStrategy((ValueError,), lambda job, exc: None)]

    def perform(self, value: int) -> None:
        raise ValueError(f"discard {value}")

    def after_discard(self) -> None:
        type(self).discard_events.append("after_discard")


def _discard_and_reraise(job, exc):
    DiscardReraisingJob.discard_events.append(f"handler {exc}")
    raise exc


class DiscardReraisingJob(quebec.BaseClass):
    discard_events: list[str] = []
    discard_on = [quebec.DiscardStrategy((ValueError,), _discard_and_reraise)]

    def perform(self, value: int) -> None:
        raise ValueError(f"discard {value}")

    def after_discard(self) -> None:
        type(self).discard_events.append("after_discard")


def _discard_and_raise_other(job, exc):
    raise RuntimeError(f"discard handler saw {exc}")


class DiscardHandlerRaisesOtherJob(quebec.BaseClass):
    discard_on = [quebec.DiscardStrategy((ValueError,), _discard_and_raise_other)]

    def perform(self, value: int) -> None:
        raise ValueError(f"original {value}")


class RescuingJob(quebec.BaseClass):
    rescued: list[str] = []
    rescue_from = [
        quebec.RescueStrategy(
            (ValueError,),
            lambda job, exc: RescuingJob.rescued.append(str(exc)),
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"rescued {value}")


def test_retry_handler_runs_on_exhaustion_without_failed_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    RetryExhaustingJob.handled = []
    qc.register_job(RetryExhaustingJob)

    RetryExhaustingJob.perform_later(qc, 7)
    execution = qc.drain_one()
    execution.perform()

    # attempts=1 means the first failure is already exhausted: the handler
    # fires once with (job, error) and the job completes without a retry or a
    # failed execution.
    assert RetryExhaustingJob.handled == [("RetryExhaustingJob", "boom 7")]
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_scheduled_executions() == 0
    assert db_assert.count_failed_executions() == 0


def test_retry_handler_does_not_fire_on_intermediate_attempt(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    RetryWithAttemptsJob.handled = []
    qc.register_job(RetryWithAttemptsJob)

    RetryWithAttemptsJob.perform_later(qc, 9)
    execution = qc.drain_one()
    execution.perform()

    # attempts=3: the first failure still has attempts left, so it takes the
    # Retry path — the handler must NOT fire, a retry is scheduled, and no
    # failed execution is recorded.
    assert RetryWithAttemptsJob.handled == []
    assert db_assert.count_scheduled_executions() == 1
    assert db_assert.count_failed_executions() == 0


def test_mixed_exception_tuple_still_matches_valid_class(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    MixedSpecJob.handled = []
    qc.register_job(MixedSpecJob)

    MixedSpecJob.perform_later(qc, 3)
    execution = qc.drain_one()
    execution.perform()

    # The ValueError entry matches even though the tuple also contains a bogus
    # non-class entry; the handler fires on exhaustion (attempts=1).
    assert MixedSpecJob.handled == ["mixed 3"]
    assert db_assert.count_failed_executions() == 0
    assert db_assert.count_scheduled_executions() == 0


def test_retry_handler_reraise_records_failed_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    RetryReraisingJob.handled = []
    qc.register_job(RetryReraisingJob)

    RetryReraisingJob.perform_later(qc, 5)
    execution = qc.drain_one()
    execution.perform()

    # The handler ran, then its re-raise propagated: the job fails like any
    # unhandled error instead of being marked finished.
    assert RetryReraisingJob.handled == ["reraise 5"]
    assert db_assert.count_scheduled_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 1
    error = _failed_error(qc_with_sqlalchemy)
    assert error["exception_class"] == "ValueError"
    assert error["message"] == "reraise 5"


def test_retry_handler_error_is_the_recorded_failure(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(RetryHandlerRaisesOtherJob)

    enqueued = RetryHandlerRaisesOtherJob.perform_later(qc, 6)
    execution = qc.drain_one()
    execution.perform()

    # As in ActiveJob, the exception escaping the handler is what fails the job.
    assert db_assert.count_failed_executions() == 1
    error = _failed_error(qc_with_sqlalchemy)
    assert error["exception_class"] == "RuntimeError"
    assert error["message"] == "handler saw original 6"
    # The failure still counts against the job's own error, so the handler's
    # RuntimeError never spends a retry_on budget of its own.
    payload = json.loads(db_assert.get_job(enqueued.id)["arguments"])
    assert payload["exception_executions"] == {"[ValueError]": 1}


def test_discard_strategy_marks_job_finished_without_failed_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    DiscardingJob.discard_events = []
    qc.register_job(DiscardingJob)

    DiscardingJob.perform_later(qc, 4)
    execution = qc.drain_one()
    execution.perform()

    assert DiscardingJob.discard_events == ["after_discard"]
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 0


def test_discard_handler_reraise_records_failed_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    DiscardReraisingJob.discard_events = []
    qc.register_job(DiscardReraisingJob)

    DiscardReraisingJob.perform_later(qc, 2)
    execution = qc.drain_one()
    execution.perform()

    # As in ActiveJob, an exception escaping the discard handler fails the job
    # and skips after_discard.
    assert DiscardReraisingJob.discard_events == ["handler discard 2"]
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 1
    error = _failed_error(qc_with_sqlalchemy)
    assert error["exception_class"] == "ValueError"
    assert error["message"] == "discard 2"


def test_discard_handler_error_is_recorded_but_not_counted(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(DiscardHandlerRaisesOtherJob)

    enqueued = DiscardHandlerRaisesOtherJob.perform_later(qc, 3)
    execution = qc.drain_one()
    execution.perform()

    # The handler's RuntimeError is recorded as the failure, but it is counted
    # against the job's own ValueError, never a RuntimeError retry_on budget.
    error = _failed_error(qc_with_sqlalchemy)
    assert error["exception_class"] == "RuntimeError"
    assert error["message"] == "discard handler saw original 3"
    payload = json.loads(db_assert.get_job(enqueued.id)["arguments"])
    assert payload["exception_executions"] == {"[ValueError]": 1}


def test_rescue_strategy_finishes_job_without_failed_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    RescuingJob.rescued = []
    qc.register_job(RescuingJob)

    RescuingJob.perform_later(qc, 8)
    execution = qc.drain_one()
    execution.perform()

    assert RescuingJob.rescued == ["rescued 8"]
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_claimed_executions() == 0
    assert db_assert.count_failed_executions() == 0


def _rescue_and_raise_other(job, exc):
    raise RuntimeError(f"rescue handler saw {exc}")


class RescueHandlerRaisesJob(quebec.BaseClass):
    rescue_from = [quebec.RescueStrategy((ValueError,), _rescue_and_raise_other)]

    def perform(self, value: int) -> None:
        raise ValueError(f"original {value}")


def test_rescue_handler_error_is_the_recorded_failure(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(RescueHandlerRaisesJob)

    enqueued = RescueHandlerRaisesJob.perform_later(qc, 11)
    execution = qc.drain_one()
    execution.perform()

    # As in ActiveJob, the exception escaping the rescue_from handler is what
    # fails the job, not the error it was rescuing.
    error = db_assert.failed_execution_error(enqueued.id)
    assert error["exception_class"] == "RuntimeError"
    assert error["message"] == "rescue handler saw original 11"


class RescueHandlerRaisesRetriedErrorJob(quebec.BaseClass):
    # The rescue handler raises an error that has its own retry_on budget.
    attempts_seen = 0
    retry_on = [
        quebec.RetryStrategy(
            (RuntimeError,), wait=timedelta(seconds=1), attempts=2, handler=None
        )
    ]
    rescue_from = [quebec.RescueStrategy((ValueError,), _rescue_and_raise_other)]

    def perform(self, value: int) -> None:
        type(self).attempts_seen += 1
        if type(self).attempts_seen == 1:
            raise ValueError(f"original {value}")
        raise RuntimeError(f"retried {value}")


def test_rescue_handler_error_does_not_spend_its_retry_budget(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]

    RescueHandlerRaisesRetriedErrorJob.attempts_seen = 0
    qc.register_job(RescueHandlerRaisesRetriedErrorJob)

    enqueued = RescueHandlerRaisesRetriedErrorJob.perform_later(qc, 4)
    qc.drain_one().perform()
    session.expire_all()

    # The handler's RuntimeError is what gets recorded, but the failure is
    # counted against the job's own ValueError, not RuntimeError's retry_on.
    assert db_assert.failed_execution_error(enqueued.id)["exception_class"] == (
        "RuntimeError"
    )
    payload = db_assert.job_arguments(enqueued.id)
    assert payload["exception_executions"] == {"[ValueError]": 1}

    # After a manual retry, the first RuntimeError still gets its retry.
    assert qc.retry_failed(enqueued.id) is True
    qc.drain_one().perform()
    session.expire_all()

    assert db_assert.count_scheduled_executions() == 1
    assert db_assert.count_failed_executions() == 0
