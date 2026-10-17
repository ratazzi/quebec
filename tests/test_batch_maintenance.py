"""Batch maintenance: `sweep_stalled_batches`, the dispatcher's
`batch_maintenance` sweep, and clearing finished batches."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import quebec
from sqlalchemy import text

from .helpers import wait_until


class Member(quebec.BaseClass):
    def perform(self) -> None:
        pass


class OnFinish(quebec.BaseClass):
    def perform(self) -> None:
        pass


@pytest.fixture
def env(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(Member)
    qc.register_job(OnFinish)
    return qc_with_sqlalchemy


def _exec(env, sql: str, **params) -> None:
    env["session"].execute(text(sql.format(p=env["prefix"])), params)
    env["session"].commit()


def _count(env, table: str, where: str = "") -> int:
    env["session"].expire_all()
    return (
        env["session"]
        .execute(text(f"SELECT COUNT(*) FROM {env['prefix']}_{table} {where}"))
        .scalar()
    )


def _queue_yml(tmp_path, body: str) -> str:
    path = tmp_path / "queue.yml"
    path.write_text(body)
    return str(path)


def _past(seconds: int) -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=seconds)


def test_sweep_removes_stale_rows_for_finished_jobs(env) -> None:
    qc = env["qc"]
    with qc.batch(on_finish=OnFinish) as batch:
        job = Member.perform_later(qc)

    # Simulate a crash between the terminal write and the release: the job is
    # finished but its tracking row is still there.
    _exec(
        env,
        "UPDATE {p}_jobs SET finished_at = :now WHERE id = :id",
        now=_past(0),
        id=job.id,
    )
    _exec(env, "DELETE FROM {p}_ready_executions")
    assert _count(env, "batch_executions") == 1
    assert not batch.reload().finished

    stats = qc.sweep_stalled_batches()
    assert stats == {"stale_executions": 1, "finished_batches": 1, "started_batches": 0}
    assert _count(env, "batch_executions") == 0
    assert batch.reload().succeeded
    assert _count(env, "jobs", "WHERE class_name = 'OnFinish'") == 1

    assert qc.sweep_stalled_batches() == {
        "stale_executions": 0,
        "finished_batches": 0,
        "started_batches": 0,
    }


def test_sweep_removes_stale_rows_for_failed_jobs(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        job = Member.perform_later(qc)

    _exec(env, "DELETE FROM {p}_ready_executions")
    _exec(
        env,
        "INSERT INTO {p}_failed_executions (job_id, error, created_at) VALUES (:id, 'boom', :now)",
        id=job.id,
        now=_past(0),
    )

    stats = qc.sweep_stalled_batches()
    assert stats["stale_executions"] == 1
    assert stats["finished_batches"] == 1
    batch.reload()
    assert batch.failed
    assert batch.failed_jobs == 1
    assert batch.completed_jobs == 0


def test_sweep_finishes_started_batch_without_rows(env) -> None:
    qc = env["qc"]
    with qc.batch(on_finish=OnFinish) as batch:
        Member.perform_later(qc)

    # A bulk delete that bypassed the release (cascade removed the row).
    _exec(env, "DELETE FROM {p}_batch_executions")
    assert not batch.reload().finished

    stats = qc.sweep_stalled_batches()
    assert stats["finished_batches"] == 1
    assert batch.reload().succeeded
    assert _count(env, "jobs", "WHERE class_name = 'OnFinish'") == 1


def test_sweep_starts_batches_stalled_before_start(env) -> None:
    qc = env["qc"]
    stale = qc._batch_create(None, None, None, None, None)  # created, never started
    fresh = qc._batch_create(None, None, None, None, None)
    _exec(
        env,
        "UPDATE {p}_batches SET created_at = :old WHERE id = :id",
        old=_past(600),
        id=stale.id,
    )

    stats = qc.sweep_stalled_batches(stalled_for=300)
    assert stats["started_batches"] == 1
    assert stats["finished_batches"] == 1  # empty, so it finished on start
    assert qc.find_batch(stale.id).succeeded
    assert qc.find_batch(fresh.id).status == "pending"

    assert qc.sweep_stalled_batches(stalled_for=0)["started_batches"] == 1
    assert qc.find_batch(fresh.id).succeeded


def test_sweep_rejects_bad_arguments(env) -> None:
    with pytest.raises(ValueError):
        env["qc"].sweep_stalled_batches(stalled_for=-1)


def test_clear_finished_batches_keeps_failed_and_unfinished(env) -> None:
    qc = env["qc"]
    with qc.batch() as succeeded:
        Member.perform_later(qc)
    with qc.batch() as failed:
        Member.perform_later(qc)
    with qc.batch() as running:
        Member.perform_later(qc)

    qc.drain_one().perform()  # the first batch's job
    assert succeeded.reload().succeeded
    # Make the second batch fail without running its job through the worker.
    _exec(env, "DELETE FROM {p}_batch_executions WHERE batch_id = :id", id=failed.id)
    _exec(
        env,
        "INSERT INTO {p}_failed_executions (job_id, error, created_at) "
        "SELECT id, 'boom', :now FROM {p}_jobs WHERE batch_id = :id",
        now=_past(0),
        id=failed.id,
    )
    qc.sweep_stalled_batches()
    assert failed.reload().failed
    assert not running.reload().finished

    # Nothing is old enough yet.
    assert qc.clear_finished_batches() == 0
    assert qc.clear_finished_batches(finished_before=4102444800.0) == 1
    assert qc.find_batch(succeeded.id) is None
    assert qc.find_batch(failed.id) is not None
    assert qc.find_batch(running.id) is not None
    assert _count(env, "batch_executions") == 1  # the running batch's row


def test_clear_finished_batches_is_separate_from_jobs(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        Member.perform_later(qc)
    qc.drain_one().perform()
    assert batch.reload().succeeded
    _exec(env, "UPDATE {p}_batches SET finished_at = :old", old=_past(30 * 86400))
    _exec(env, "UPDATE {p}_jobs SET finished_at = :old", old=_past(30 * 86400))

    assert qc.clear_finished_jobs() == 1
    # The Python API only clears jobs; batches are cleared by the periodic
    # worker cleanup or explicitly.
    assert qc.find_batch(batch.id) is not None
    assert qc.clear_finished_batches() == 1
    assert qc.find_batch(batch.id) is None


def test_batch_maintenance_defaults_true(monkeypatch, db_url, test_prefix) -> None:
    monkeypatch.delenv("QUEBEC_CONFIG", raising=False)
    monkeypatch.delenv("QUEBEC_ENV", raising=False)
    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert inst._dispatcher_batch_maintenance() is True
    finally:
        inst.close()


def test_batch_maintenance_false_from_yml(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - batch_maintenance: false
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)
    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert inst._dispatcher_batch_maintenance() is False
        assert inst._dispatcher_concurrency_maintenance() is True
    finally:
        inst.close()


def test_batch_maintenance_kwarg_and_env(monkeypatch, db_url, test_prefix) -> None:
    monkeypatch.delenv("QUEBEC_CONFIG", raising=False)
    inst = quebec.Quebec(
        db_url, table_name_prefix=test_prefix, dispatcher_batch_maintenance=False
    )
    try:
        assert inst._dispatcher_batch_maintenance() is False
    finally:
        inst.close()
    monkeypatch.setenv("QUEBEC_DISPATCHER_BATCH_MAINTENANCE", "false")
    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert inst._dispatcher_batch_maintenance() is False
    finally:
        inst.close()


def test_dispatcher_sweep_repairs_stale_row(env) -> None:
    """The maintenance timer's first tick fires on dispatcher start, so the
    sweep repairs a leaked tracking row well before the 600s interval."""
    qc = env["qc"]
    with qc.batch(on_finish=OnFinish) as batch:
        job = Member.perform_later(qc)
    _exec(
        env,
        "UPDATE {p}_jobs SET finished_at = :now WHERE id = :id",
        now=_past(0),
        id=job.id,
    )
    _exec(env, "DELETE FROM {p}_ready_executions")
    assert not batch.reload().finished

    qc.spawn_dispatcher()
    wait_until(
        lambda: batch.reload().finished,
        timeout=5,
        message="dispatcher batch sweep did not finish the batch",
    )
    assert batch.succeeded
    assert _count(env, "jobs", "WHERE class_name = 'OnFinish'") == 1
