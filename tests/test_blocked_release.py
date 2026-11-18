"""Regression tests for releasing blocked jobs.

1. The control plane's unblock actions used to delete the blocked_execution
   without creating a ready_execution, orphaning the job forever.
2. ``qc.unblock()`` used to promote a job without acquiring its concurrency
   semaphore. When that job finished it released a slot it never held, so a
   later job ran next to the current holder and broke the limit.

Both now follow Solid Queue's ``BlockedExecution#release``: a blocked job is
promoted only after acquiring the semaphore, and otherwise stays blocked until
a slot frees up.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

import quebec


class GuardedJob(quebec.ActiveJob):
    queue_as = "default"
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "guarded-resource"

    def perform(self, *args, **kwargs):
        pass


class OtherGuardedJob(quebec.ActiveJob):
    queue_as = "default"
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "other-guarded-resource"

    def perform(self, *args, **kwargs):
        pass


def _scalar(session, sql):
    session.expire_all()
    return session.execute(text(sql)).scalar()


def _state(session, prefix, job_id):
    """Which execution table holds ``job_id``, plus its finished flag."""
    tables = ("ready", "blocked", "claimed", "failed", "scheduled")
    present = [
        t
        for t in tables
        if _scalar(
            session,
            f"SELECT COUNT(*) FROM {prefix}_{t}_executions WHERE job_id = {job_id}",
        )
    ]
    finished = (
        _scalar(
            session,
            f"SELECT COUNT(*) FROM {prefix}_jobs "
            f"WHERE id = {job_id} AND finished_at IS NOT NULL",
        )
        == 1
    )
    return present, finished


def _blocked_execution_id(session, prefix, job_id):
    return _scalar(
        session,
        f"SELECT id FROM {prefix}_blocked_executions WHERE job_id = {job_id}",
    )


def _post(qc, path):
    req = quebec.AsgiRequest("POST", path, "", [], b"", "")
    status, _headers, _body = qc.handle_control_plane_request(req)
    return status


def _run_next(qc):
    execution = qc.drain_one()
    assert execution is not None
    execution.perform()
    return execution


# --- 1. control plane unblock -------------------------------------------------


@pytest.fixture
def env(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(GuardedJob)
    qc.register_job(OtherGuardedJob)
    return qc, qc_with_sqlalchemy["session"], qc_with_sqlalchemy["prefix"]


def test_control_plane_unblock_while_slot_held_keeps_job_runnable(env) -> None:
    qc, session, prefix = env
    holder = GuardedJob.perform_later(qc)
    waiter = GuardedJob.perform_later(qc)
    blocked_id = _blocked_execution_id(session, prefix, waiter.id)

    assert _post(qc, f"/blocked-jobs/{blocked_id}/unblock") == 303

    # No free slot: the job must stay blocked, not vanish from every queue.
    assert _state(session, prefix, waiter.id) == (["blocked"], False)

    # Once the holder finishes, the waiter is handed the slot and runs.
    _run_next(qc)
    assert _state(session, prefix, holder.id) == ([], True)
    assert _state(session, prefix, waiter.id) == (["ready"], False)
    _run_next(qc)
    assert _state(session, prefix, waiter.id) == ([], True)


def test_control_plane_unblock_with_free_slot_promotes_to_ready(env) -> None:
    qc, session, prefix = env
    GuardedJob.perform_later(qc)
    waiter = GuardedJob.perform_later(qc)
    blocked_id = _blocked_execution_id(session, prefix, waiter.id)
    # The expired-semaphore sweep frees the slot but has not released the
    # waiter yet: this is the case an operator's unblock is for.
    session.execute(text(f"DELETE FROM {prefix}_semaphores"))
    session.commit()

    assert _post(qc, f"/blocked-jobs/{blocked_id}/unblock") == 303

    assert _state(session, prefix, waiter.id) == (["ready"], False)
    assert _scalar(session, f"SELECT value FROM {prefix}_semaphores") == 0


def test_control_plane_unblock_unknown_id_is_not_found(env) -> None:
    qc, _session, _prefix = env
    assert _post(qc, "/blocked-jobs/999999/unblock") == 404


def test_control_plane_unblock_all_never_orphans_jobs(env) -> None:
    qc, session, prefix = env
    GuardedJob.perform_later(qc)
    waiter = GuardedJob.perform_later(qc)
    OtherGuardedJob.perform_later(qc)
    other_waiter = OtherGuardedJob.perform_later(qc)
    # Free only the other key's slot.
    key_col = "`key`" if session.bind.dialect.name == "mysql" else '"key"'
    session.execute(
        text(
            f"DELETE FROM {prefix}_semaphores "
            f"WHERE {key_col} = 'OtherGuardedJob/other-guarded-resource'"
        )
    )
    session.commit()

    assert _post(qc, "/blocked-jobs/all/unblock") == 303

    # Held slot: stays blocked. Freed slot: promoted. Neither is orphaned.
    assert _state(session, prefix, waiter.id) == (["blocked"], False)
    assert _state(session, prefix, other_waiter.id) == (["ready"], False)

    # Drain everything: every job finishes, nothing is stranded.
    for _ in range(4):
        _run_next(qc)
    assert (
        _scalar(
            session, f"SELECT COUNT(*) FROM {prefix}_jobs WHERE finished_at IS NULL"
        )
        == 0
    )


# --- 2. qc.unblock() honours the semaphore ---------------------------------------


def test_unblock_while_slot_held_does_not_break_the_limit(env) -> None:
    qc, session, prefix = env
    a = GuardedJob.perform_later(qc)
    b = GuardedJob.perform_later(qc)

    # A holds the only slot, so B cannot be released.
    assert qc.unblock(b.id) is False
    assert _state(session, prefix, b.id) == (["blocked"], False)

    # A runs and hands its slot to B.
    _run_next(qc)
    assert _state(session, prefix, a.id) == ([], True)
    assert _state(session, prefix, b.id) == (["ready"], False)

    # While B holds the slot, a newcomer must block rather than run beside it.
    c = GuardedJob.perform_later(qc)
    assert _state(session, prefix, c.id) == (["blocked"], False)

    execution = qc.drain_one()
    assert _state(session, prefix, b.id) == (["claimed"], False)
    # B is mid-flight: C must remain blocked.
    assert _state(session, prefix, c.id) == (["blocked"], False)
    execution.perform()
    assert _state(session, prefix, c.id) == (["ready"], False)


def test_unblock_all_while_slots_held_releases_nothing(env) -> None:
    qc, session, prefix = env
    GuardedJob.perform_later(qc)
    b = GuardedJob.perform_later(qc)
    OtherGuardedJob.perform_later(qc)
    d = OtherGuardedJob.perform_later(qc)

    assert qc.unblock_all() == 0
    assert _state(session, prefix, b.id) == (["blocked"], False)
    assert _state(session, prefix, d.id) == (["blocked"], False)
    assert _scalar(session, f"SELECT SUM(value) FROM {prefix}_semaphores") == 0
