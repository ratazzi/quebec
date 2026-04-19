from __future__ import annotations

import quebec


def test_register_job_allows_convenience_perform_later_without_explicit_qc(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    class RegisteredJob(quebec.BaseClass):
        def perform(self, value: int) -> None:
            return None

    qc.register_job(RegisteredJob)

    enqueued = RegisteredJob.perform_later(5)

    assert enqueued.id is not None
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 1


def test_lifecycle_decorator_registration_returns_original_handler(qc) -> None:
    events: list[str] = []

    @qc.on_start
    def start_handler() -> None:
        events.append("start")

    @qc.on_stop
    def stop_handler() -> None:
        events.append("stop")

    @qc.on_shutdown
    def shutdown_handler() -> None:
        events.append("shutdown")

    assert start_handler.__name__ == "start_handler"
    assert stop_handler.__name__ == "stop_handler"
    assert shutdown_handler.__name__ == "shutdown_handler"
    assert events == []
