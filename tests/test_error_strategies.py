from __future__ import annotations

import quebec


class DiscardingJob(quebec.BaseClass):
    discard_events: list[str] = []
    discard_on = [quebec.DiscardStrategy((ValueError,), lambda exc: None)]

    def perform(self, value: int) -> None:
        raise ValueError(f"discard {value}")

    def after_discard(self) -> None:
        type(self).discard_events.append("after_discard")


class RescuingJob(quebec.BaseClass):
    rescued: list[str] = []
    rescue_from = [
        quebec.RescueStrategy(
            (ValueError,),
            lambda exc: RescuingJob.rescued.append(str(exc)),
        )
    ]

    def perform(self, value: int) -> None:
        raise ValueError(f"rescued {value}")


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
