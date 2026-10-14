"""after_discard follows ActiveJob 7.1: it runs whenever a job is given up for
good (discarded, retries exhausted, or failed with nothing handling the error),
and not when the error is rescued, a retry is scheduled, or a handler raises."""

from __future__ import annotations

from datetime import timedelta

import quebec

events: list[tuple[str, str]] = []


def _record(job, error) -> None:
    events.append((type(job).__name__, str(error)))


class UnhandledJob(quebec.BaseClass):
    def perform(self, value: int) -> None:
        raise ValueError(f"unhandled {value}")

    def after_discard(self, error) -> None:
        _record(self, error)


class LegacyHookJob(quebec.BaseClass):
    # The older zero-argument form keeps working.
    def perform(self, value: int) -> None:
        raise ValueError(f"legacy {value}")

    def after_discard(self) -> None:
        events.append((type(self).__name__, "no error"))


class ExhaustedNoHandlerJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,), wait=timedelta(seconds=1), attempts=1, handler=None
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"exhausted {value}")

    def after_discard(self, error) -> None:
        _record(self, error)


class ExhaustedWithHandlerJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=1),
            attempts=1,
            handler=lambda job, exc: events.append(("handler", str(exc))),
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"exhausted {value}")

    def after_discard(self, error) -> None:
        _record(self, error)


class RetryScheduledJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,), wait=timedelta(seconds=1), attempts=3, handler=None
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"retry {value}")

    def after_discard(self, error) -> None:
        _record(self, error)


class DiscardedJob(quebec.BaseClass):
    discard_on = [quebec.DiscardStrategy((ValueError,), None)]

    def perform(self, value: int) -> None:
        raise ValueError(f"discarded {value}")

    def after_discard(self, error) -> None:
        _record(self, error)


class RescuedJob(quebec.BaseClass):
    rescue_from = [quebec.RescueStrategy((ValueError,), lambda job, exc: None)]

    def perform(self, value: int) -> None:
        raise ValueError(f"rescued {value}")

    def after_discard(self, error) -> None:
        _record(self, error)


def _reraise(job, exc):
    raise exc


class HandlerRaisesJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,), wait=timedelta(seconds=1), attempts=1, handler=_reraise
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"reraised {value}")

    def after_discard(self, error) -> None:
        _record(self, error)


class HookRaisesJob(quebec.BaseClass):
    discard_on = [quebec.DiscardStrategy((ValueError,), None)]

    def perform(self, value: int) -> None:
        raise ValueError(f"original {value}")

    def after_discard(self, error) -> None:
        raise RuntimeError(f"hook saw {error}")


def _run(qc, job_class, value: int):
    events.clear()
    qc.register_job(job_class)
    enqueued = job_class.perform_later(qc, value)
    qc.drain_one().perform()
    return enqueued


def test_unhandled_failure_runs_after_discard_and_fails(
    qc_with_sqlalchemy, db_assert
) -> None:
    enqueued = _run(qc_with_sqlalchemy["qc"], UnhandledJob, 1)

    assert events == [("UnhandledJob", "unhandled 1")]
    assert db_assert.failed_execution_error(enqueued.id)["message"] == "unhandled 1"


def test_zero_argument_after_discard_still_runs(qc_with_sqlalchemy, db_assert) -> None:
    _run(qc_with_sqlalchemy["qc"], LegacyHookJob, 2)

    assert events == [("LegacyHookJob", "no error")]
    assert db_assert.count_failed_executions() == 1


def test_exhausted_retries_without_handler_run_after_discard_and_fail(
    qc_with_sqlalchemy, db_assert
) -> None:
    enqueued = _run(qc_with_sqlalchemy["qc"], ExhaustedNoHandlerJob, 3)

    # The job still lands in failed executions, so it can be retried by hand.
    assert events == [("ExhaustedNoHandlerJob", "exhausted 3")]
    assert db_assert.failed_execution_error(enqueued.id)["message"] == "exhausted 3"


def test_exhausted_retries_with_handler_run_after_discard_after_the_handler(
    qc_with_sqlalchemy, db_assert
) -> None:
    _run(qc_with_sqlalchemy["qc"], ExhaustedWithHandlerJob, 4)

    assert events == [
        ("handler", "exhausted 4"),
        ("ExhaustedWithHandlerJob", "exhausted 4"),
    ]
    assert db_assert.count_failed_executions() == 0


def test_discard_runs_after_discard_with_the_error(
    qc_with_sqlalchemy, db_assert
) -> None:
    _run(qc_with_sqlalchemy["qc"], DiscardedJob, 5)

    assert events == [("DiscardedJob", "discarded 5")]
    assert db_assert.count_failed_executions() == 0


def test_scheduled_retry_does_not_run_after_discard(
    qc_with_sqlalchemy, db_assert
) -> None:
    _run(qc_with_sqlalchemy["qc"], RetryScheduledJob, 6)

    assert events == []
    assert db_assert.count_scheduled_executions() == 1


def test_rescued_error_does_not_run_after_discard(
    qc_with_sqlalchemy, db_assert
) -> None:
    _run(qc_with_sqlalchemy["qc"], RescuedJob, 7)

    assert events == []
    assert db_assert.count_failed_executions() == 0


def test_raising_handler_skips_after_discard(qc_with_sqlalchemy, db_assert) -> None:
    _run(qc_with_sqlalchemy["qc"], HandlerRaisesJob, 8)

    # The handler's exception escapes before after_discard would run.
    assert events == []
    assert db_assert.count_failed_executions() == 1


def test_after_discard_error_fails_the_job(qc_with_sqlalchemy, db_assert) -> None:
    enqueued = _run(qc_with_sqlalchemy["qc"], HookRaisesJob, 9)

    # As in ActiveJob, the hook's exception propagates: the discarded job fails
    # with it, still counted against the job's own error.
    error = db_assert.failed_execution_error(enqueued.id)
    assert error["exception_class"] == "RuntimeError"
    assert error["message"] == "hook saw original 9"
    assert db_assert.job_arguments(enqueued.id)["exception_executions"] == {
        "[ValueError]": 1
    }
