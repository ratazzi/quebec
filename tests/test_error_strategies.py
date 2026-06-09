from __future__ import annotations

from datetime import timedelta

import quebec


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


class DiscardingJob(quebec.BaseClass):
    discard_events: list[str] = []
    discard_on = [quebec.DiscardStrategy((ValueError,), lambda job, exc: None)]

    def perform(self, value: int) -> None:
        raise ValueError(f"discard {value}")

    def after_discard(self) -> None:
        type(self).discard_events.append("after_discard")


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
