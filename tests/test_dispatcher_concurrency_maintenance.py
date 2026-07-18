"""queue.yml `concurrency_maintenance: false` disables the dispatcher's
concurrency-maintenance sweep (previously parsed but never read), and the
sweep itself runs on `concurrency_maintenance_interval`, not every poll."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import quebec
from sqlalchemy import text

from .helpers import wait_until


class SweepConcurrentJob(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "sweep-resource"

    def perform(self) -> None:
        pass


def _queue_yml(tmp_path, body: str) -> str:
    path = tmp_path / "queue.yml"
    path.write_text(body)
    return str(path)


def test_concurrency_maintenance_defaults_true(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.delenv("QUEBEC_CONFIG", raising=False)
    monkeypatch.delenv("QUEBEC_ENV", raising=False)
    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert inst._dispatcher_concurrency_maintenance() is True
    finally:
        inst.close()


def test_concurrency_maintenance_false_is_honored(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - concurrency_maintenance: false
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)
    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert inst._dispatcher_concurrency_maintenance() is False
    finally:
        inst.close()


def test_maintenance_sweep_reclaims_crashed_holder(
    qc_with_sqlalchemy, db_assert
) -> None:
    """The maintenance sweep deletes expired semaphores and promotes expired
    blocked executions — the crash-recovery fallback (the normal completion
    path unblocks directly in ``after_executed``). The sweep's first tick
    fires immediately on dispatcher start (Solid Queue ``run_now: true``
    parity), so this must succeed well before the 600s default interval."""
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(SweepConcurrentJob)

    # A holds the semaphore (ready), B is blocked on the same key.
    SweepConcurrentJob.perform_later(qc)
    SweepConcurrentJob.perform_later(qc)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 1

    # Simulate a crashed holder: A's ready row is gone and both the semaphore
    # and B's blocked row have passed their TTL.
    past = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=60)
    session.execute(text(f"DELETE FROM {prefix}_ready_executions"))
    session.execute(
        text(f"UPDATE {prefix}_semaphores SET expires_at = :past"), {"past": past}
    )
    session.execute(
        text(f"UPDATE {prefix}_blocked_executions SET expires_at = :past"),
        {"past": past},
    )
    session.commit()

    qc.spawn_dispatcher()
    wait_until(
        lambda: (session.expire_all() or True)
        and db_assert.count_blocked_executions() == 0
        and db_assert.count_ready_executions() == 1,
        timeout=5,
        message="maintenance sweep did not reclaim the crashed holder",
    )
