"""Tests for concurrency control: semaphore, blocked executions, unblock flow."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import quebec
from sqlalchemy import text

from .helpers import wait_until


class ConcurrentJob(quebec.BaseClass):
    concurrency_limit = 1
    calls: list[int] = []

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "exclusive-resource"

    def perform(self, value: int) -> None:
        type(self).calls.append(value)


class RetryConcurrentJob(quebec.BaseClass):
    concurrency_limit = 1
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=0),
            attempts=2,
            handler=None,
        )
    ]

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "exclusive-resource"

    def perform(self, value: int) -> None:
        raise ValueError(f"retry me: {value}")


def test_first_job_goes_to_ready_second_goes_to_blocked(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)
    ConcurrentJob.perform_later(qc, 2)

    assert db_assert.count_jobs() == 2
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 1


def test_completing_first_job_releases_semaphore(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)

    # Semaphore value=0 (acquired)
    sem = session.execute(text(f"SELECT value FROM {prefix}_semaphores")).fetchone()
    assert sem.value == 0

    # Execute first job
    execution = qc.drain_one()
    execution.perform()
    session.expire_all()

    assert ConcurrentJob.calls == [1]
    # Semaphore released (value back to 1)
    sem = session.execute(text(f"SELECT value FROM {prefix}_semaphores")).fetchone()
    assert sem.value == 1


def test_semaphore_created_on_first_concurrent_job(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)

    semaphores = session.execute(text(f"SELECT * FROM {prefix}_semaphores")).fetchall()
    assert len(semaphores) == 1
    assert semaphores[0].value == 0  # limit=1, one acquired → value=0


class DiscardOnConflictJob(quebec.BaseClass):
    concurrency_limit = 1
    concurrency_on_conflict = quebec.ConcurrencyConflict.Discard

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "singleton"

    def perform(self, value: int) -> None:
        return None


def test_discard_on_conflict_discards_second_job(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(DiscardOnConflictJob)

    DiscardOnConflictJob.perform_later(qc, 1)
    DiscardOnConflictJob.perform_later(qc, 2)

    # First goes to ready, second is discarded (not blocked)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0
    # Discarded job should be finished (not failed)
    assert db_assert.count_failed_executions() == 0


def test_blocked_job_promoted_immediately_after_release_with_limit_one(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Regression: with concurrency_limit=1, completing job A must release the
    semaphore in a way that lets the blocked job B be promoted to ready right
    away — NOT after ``concurrency_duration`` seconds.

    Previously ``acquire_semaphore`` short-circuited to ``false`` whenever the
    semaphore row existed with ``concurrency_limit == 1``, so the row had to
    be physically deleted by the dispatcher's expired-key sweep before any
    new acquire could succeed. That turned ``concurrency_duration`` into a
    minimum cooldown instead of a crash-safety TTL.
    """
    qc = qc_with_sqlalchemy["qc"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    # A → ready, B → blocked.
    ConcurrentJob.perform_later(qc, 1)
    ConcurrentJob.perform_later(qc, 2)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 1

    # Drain A. Its release path (`release_next_blocked_job`) must promote B.
    execution_a = qc.drain_one()
    execution_a.perform()

    # B is now claimable — without waiting on concurrency_duration.
    assert db_assert.count_blocked_executions() == 0
    assert db_assert.count_ready_executions() == 1

    execution_b = qc.drain_one()
    execution_b.perform()
    assert ConcurrentJob.calls == [1, 2]


def test_discard_mode_can_reenqueue_after_release_with_limit_one(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Regression: with concurrency_limit=1 + Discard mode, a chained
    ``perform_later`` issued right after the job finishes must land in ready,
    not be discarded. Previously the freed semaphore row was still readable
    by the ``limit == 1`` short-circuit in ``acquire_semaphore``, forcing
    every re-enqueue within ``concurrency_duration`` seconds to be discarded.
    """
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(DiscardOnConflictJob)

    DiscardOnConflictJob.perform_later(qc, 1)
    execution = qc.drain_one()
    execution.perform()

    # Job A is finished and the semaphore is released. A fresh perform_later
    # for the same concurrency_key must acquire and reach ready_executions.
    DiscardOnConflictJob.perform_later(qc, 2)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0
    # The new job is NOT marked finished (i.e. NOT discarded).
    assert db_assert.count_jobs() == 2


def test_limit_one_still_enforces_serialization_under_burst(
    qc_with_sqlalchemy, db_assert
) -> None:
    """After the limit==1 short-circuit fix, the slot must still admit
    exactly one ready/in-flight job at a time. Bursting N enqueues against a
    busy semaphore must leave 1 ready + (N-1) blocked, never N ready.
    """
    qc = qc_with_sqlalchemy["qc"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    # A occupies the slot (semaphore value=0).
    ConcurrentJob.perform_later(qc, 1)
    # Burst of additional enqueues while the slot is still held.
    for i in range(2, 6):
        ConcurrentJob.perform_later(qc, i)

    # Only the first one made it to ready; the other four are blocked.
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 4


def test_limit_one_re_enqueue_after_release_does_not_double_up(
    qc_with_sqlalchemy, db_assert
) -> None:
    """After release, re-enqueue takes the freed slot. Bursting more
    re-enqueues immediately after must still see only one in ready and the
    rest blocked — value=1 (free) is decremented atomically to 0 by the
    winning acquire, so concurrent enqueues for the same key cannot all
    land in ready.
    """
    qc = qc_with_sqlalchemy["qc"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)
    qc.drain_one().perform()  # release happens here

    # Three back-to-back enqueues against the freed slot.
    for i in range(2, 5):
        ConcurrentJob.perform_later(qc, i)

    # First grabs the slot (value 1→0), other two are blocked.
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 2


def test_limit_one_full_drain_chain_runs_serially(
    qc_with_sqlalchemy, db_assert
) -> None:
    """End-to-end: enqueue N jobs, drain them all one by one, and verify
    each completion releases the slot for the next blocked job — no job is
    ever lost, no two ever sit in ready simultaneously.
    """
    qc = qc_with_sqlalchemy["qc"]

    ConcurrentJob.calls = []
    qc.register_job(ConcurrentJob)

    for i in range(1, 6):
        ConcurrentJob.perform_later(qc, i)

    # Initial state: 1 ready, 4 blocked.
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 4

    # Drain in a tight loop; after each perform the next blocked job must
    # have been promoted to ready (never 0, never 2).
    for expected_blocked in range(4, -1, -1):
        execution = qc.drain_one()
        execution.perform()
        if expected_blocked > 0:
            assert db_assert.count_ready_executions() == 1
            assert db_assert.count_blocked_executions() == expected_blocked - 1
        else:
            assert db_assert.count_ready_executions() == 0
            assert db_assert.count_blocked_executions() == 0

    assert ConcurrentJob.calls == [1, 2, 3, 4, 5]


def test_due_scheduled_job_respects_held_semaphore(
    qc_with_sqlalchemy, db_assert
) -> None:
    """A delayed job must acquire the semaphore when dispatcher promotes it.

    This covers the wait/wait_until path: enqueue stores the row in
    scheduled_executions without acquiring, so the dispatcher promotion is
    the point where concurrency must be enforced.
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrentJob)

    ConcurrentJob.perform_later(qc, 1)
    delayed = ConcurrentJob.set(wait=3600).perform_later(qc, 2)
    due_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=10)
    session.execute(
        text(
            f"UPDATE {prefix}_scheduled_executions "
            "SET scheduled_at = :due_at "
            "WHERE job_id = :job_id"
        ),
        {"job_id": delayed.id, "due_at": due_at},
    )
    session.commit()

    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_scheduled_executions() == 1

    qc.spawn_dispatcher()
    wait_until(
        lambda: (session.expire_all() or True)
        and db_assert.count_scheduled_executions() == 0,
        timeout=5,
        message="dispatcher did not process due scheduled job",
    )

    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 1
    blocked_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions")
    ).scalar()
    assert blocked_job_id == delayed.id


def test_automatic_retry_promotion_respects_held_semaphore(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Retry backoff rows also pass through scheduled_executions.

    After the failed attempt releases its slot, another same-key job can hold
    the semaphore before the retry becomes due. Dispatcher must block the due
    retry instead of bulk-promoting it into ready.
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(RetryConcurrentJob)

    failed_attempt = RetryConcurrentJob.perform_later(qc, 1)
    execution = qc.drain_one()
    execution.perform()
    session.expire_all()

    assert db_assert.count_scheduled_executions() == 1
    assert db_assert.count_failed_executions() == 0
    assert db_assert.count_ready_executions() == 0
    retry_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_scheduled_executions")
    ).scalar()
    assert retry_job_id != failed_attempt.id

    blocker = RetryConcurrentJob.perform_later(qc, 2)
    assert db_assert.count_ready_executions() == 1

    qc.spawn_dispatcher()
    wait_until(
        lambda: (session.expire_all() or True)
        and db_assert.count_scheduled_executions() == 0,
        timeout=5,
        message="dispatcher did not process due retry job",
    )

    assert db_assert.count_ready_executions() == 1
    ready_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_ready_executions")
    ).scalar()
    assert ready_job_id == blocker.id

    assert db_assert.count_blocked_executions() == 1
    blocked_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions")
    ).scalar()
    assert blocked_job_id == retry_job_id


def _insert_failed_execution(
    session, prefix: str, class_name: str, concurrency_key: str
) -> int:
    """Create a jobs row + failed_executions row directly, simulating what
    the failed jobs list would show in the control plane.
    """
    session.execute(
        text(
            f"INSERT INTO {prefix}_jobs "
            "(queue_name, class_name, arguments, priority, concurrency_key, "
            " created_at, updated_at) "
            "VALUES ('default', :cn, '[]', 0, :ck, "
            "        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        ),
        {"cn": class_name, "ck": concurrency_key},
    )
    job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.execute(
        text(
            f"INSERT INTO {prefix}_failed_executions "
            "(job_id, error, created_at) VALUES (:jid, '{}', CURRENT_TIMESTAMP)"
        ),
        {"jid": job_id},
    )
    session.commit()
    return job_id


def test_manual_retry_blocks_when_slot_held(qc_with_sqlalchemy, db_assert) -> None:
    """Single failed retry via control plane / ``qc.retry_failed`` must
    acquire the semaphore. When the slot is already held by another job for
    the same key, the retried job must land in blocked_executions instead of
    bulk-inserting into ready (the previous bypass).
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrentJob)

    # A enqueues normally → acquires the slot, lands in ready.
    ConcurrentJob.perform_later(qc, 1)
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 0

    # Pre-populate a failed_execution sharing the same (class-prefixed) key.
    failed_id = _insert_failed_execution(
        session,
        prefix,
        class_name=ConcurrentJob.__qualname__,
        concurrency_key=f"{ConcurrentJob.__qualname__}/exclusive-resource",
    )
    session.expire_all()
    assert db_assert.count_failed_executions() == 1

    # The manual retry must NOT just bulk-insert into ready.
    assert qc.retry_failed(failed_id) is True

    session.expire_all()
    assert db_assert.count_ready_executions() == 1, "ready should still hold only A"
    assert db_assert.count_blocked_executions() == 1, "retried job must be blocked"
    assert db_assert.count_failed_executions() == 0
    blocked_job_id = session.execute(
        text(f"SELECT job_id FROM {prefix}_blocked_executions")
    ).scalar()
    assert blocked_job_id == failed_id


def test_manual_retry_all_partitions_per_key(qc_with_sqlalchemy, db_assert) -> None:
    """Bulk retry via control plane / ``qc.retry_all_failed`` must route
    each retried job through the concurrency check (Solid Queue's
    ``dispatch_all`` partition). Previously the helper bulk-inserted every
    failed job straight into ready, ignoring the held slot.
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrentJob)

    # A holds the only slot for "exclusive-resource".
    ConcurrentJob.perform_later(qc, 1)

    # Two failed_executions sharing the same key. After retry_all, the slot
    # is still A's — both must end up blocked, not ready.
    for _ in range(2):
        _insert_failed_execution(
            session,
            prefix,
            class_name=ConcurrentJob.__qualname__,
            concurrency_key=f"{ConcurrentJob.__qualname__}/exclusive-resource",
        )
    session.expire_all()
    assert db_assert.count_failed_executions() == 2
    assert db_assert.count_ready_executions() == 1  # A
    assert db_assert.count_blocked_executions() == 0

    count = qc.retry_all_failed()
    assert count == 2

    session.expire_all()
    assert db_assert.count_failed_executions() == 0
    assert db_assert.count_ready_executions() == 1, "ready still only holds A"
    assert db_assert.count_blocked_executions() == 2, (
        "both retried jobs must be blocked while A holds the slot"
    )


def test_manual_retry_all_400_same_key_only_one_ready(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Hard reproduction of the user-reported scenario: 400 failed
    executions sharing one concurrency_key. ``retry_all`` must hand exactly
    one of them the slot and route the remaining 399 to blocked, NOT bulk
    insert all 400 into ready.
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrentJob)

    for _ in range(400):
        _insert_failed_execution(
            session,
            prefix,
            class_name=ConcurrentJob.__qualname__,
            concurrency_key=f"{ConcurrentJob.__qualname__}/exclusive-resource",
        )
    session.expire_all()
    assert db_assert.count_failed_executions() == 400

    count = qc.retry_all_failed()
    assert count == 400

    session.expire_all()
    assert db_assert.count_failed_executions() == 0
    assert db_assert.count_ready_executions() == 1, (
        f"only one retried job may grab the slot, "
        f"got {db_assert.count_ready_executions()} in ready"
    )
    assert db_assert.count_blocked_executions() == 399


def test_manual_retry_all_promotes_when_slot_free(
    qc_with_sqlalchemy, db_assert
) -> None:
    """Sanity counterpart: when no job currently holds the slot, retry_all
    promotes the first matching job to ready (taking the slot), and the
    rest go to blocked. Confirms the fix doesn't over-restrict.
    """
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConcurrentJob)

    # No live A. Two failed_executions; retry_all should make exactly one
    # ready and one blocked.
    for _ in range(2):
        _insert_failed_execution(
            session,
            prefix,
            class_name=ConcurrentJob.__qualname__,
            concurrency_key=f"{ConcurrentJob.__qualname__}/exclusive-resource",
        )
    session.expire_all()

    count = qc.retry_all_failed()
    assert count == 2

    session.expire_all()
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 1
    assert db_assert.count_failed_executions() == 0
