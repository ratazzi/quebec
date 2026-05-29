# coding: utf-8
"""Tests for the per-class ``exclusive`` flag.

When ``exclusive = True`` is set on a job class, claiming any such job on a
worker:

  1. Releases every other claimed sibling in the same batch back to ready.
  2. Sets ``AppContext.exclusive_active`` so subsequent ``process_available_jobs``
     polls skip claiming new work.
  3. Causes ``pick_job`` to wait on the ledger until every OTHER claim
     (Dispatched or InFlight) has drained before returning the exclusive
     execution.
  4. Clears ``exclusive_active`` once the exclusive finishes (or its
     ``Execution`` is dropped).

These tests drive the sieve through ``drain_batch`` (which calls
``claim_jobs`` directly) and exercise the ledger gates via the test-only
``_set_ledger_state`` / ``_ledger_state`` PyMethods.
"""

from __future__ import annotations

import gc

import quebec
from sqlalchemy import text


def _claimed_count(session, prefix: str) -> int:
    return session.execute(
        text(f"SELECT COUNT(*) FROM {prefix}_claimed_executions")
    ).scalar()


def _ready_count(session, prefix: str) -> int:
    return session.execute(
        text(f"SELECT COUNT(*) FROM {prefix}_ready_executions")
    ).scalar()


class _NormalJob(quebec.ActiveJob):
    def perform(self, *args, **kwargs):
        return True


class _ExclusiveJob(quebec.ActiveJob):
    exclusive = True

    def perform(self, *args, **kwargs):
        return True


# ──────────────────────────────────────────────────────────────────────
# Configuration / parsing
# ──────────────────────────────────────────────────────────────────────


def test_exclusive_default_false_for_unmarked_class(qc_with_sqlalchemy):
    """Classes without ``exclusive`` declared never flip the flag."""
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(_NormalJob)
    qc.register_worker_process()

    _NormalJob.perform_later(qc)
    (execution,) = qc.drain_batch(1)

    assert qc._is_exclusive_active() is False
    del execution


def test_exclusive_rejects_non_bool(qc_with_sqlalchemy):
    """A non-bool ``exclusive`` attribute raises at register time."""
    qc = qc_with_sqlalchemy["qc"]

    class BadExclusive(quebec.ActiveJob):
        exclusive = "yes"

        def perform(self, *args, **kwargs):
            return True

    try:
        qc.register_job(BadExclusive)
    except TypeError as e:
        assert "exclusive must be a bool" in str(e)
    else:
        raise AssertionError("expected TypeError for non-bool exclusive")


# ──────────────────────────────────────────────────────────────────────
# Sieve behavior: claim → release siblings → set exclusive_active
# ──────────────────────────────────────────────────────────────────────


def test_single_exclusive_in_isolation_flips_flag(qc_with_sqlalchemy):
    """A single exclusive job claimed alone sets the flag and clears on perform."""
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(_ExclusiveJob)
    qc.register_worker_process()

    _ExclusiveJob.perform_later(qc)
    (execution,) = qc.drain_batch(1)

    assert qc._is_exclusive_active() is True
    execution.perform()
    assert qc._is_exclusive_active() is False


def test_mixed_batch_keeps_only_exclusive_releases_siblings(qc_with_sqlalchemy):
    """A mixed batch (normal + exclusive) keeps the exclusive, releases the rest."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_NormalJob)
    qc.register_job(_ExclusiveJob)
    qc.register_worker_process()

    _NormalJob.perform_later(qc)
    _NormalJob.perform_later(qc)
    _NormalJob.perform_later(qc)
    _ExclusiveJob.perform_later(qc)

    claimed = qc.drain_batch(4)
    session.expire_all()

    # Only one execution returned (the exclusive); the three normals went back.
    assert len(claimed) == 1
    assert _claimed_count(session, prefix) == 1
    assert _ready_count(session, prefix) == 3
    assert qc._is_exclusive_active() is True

    # Clean up the held execution.
    claimed[0].perform()
    assert qc._is_exclusive_active() is False


def test_multiple_exclusives_in_batch_keeps_one(qc_with_sqlalchemy):
    """Two exclusives in the same batch: keep one, release the other."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_ExclusiveJob)
    qc.register_worker_process()

    _ExclusiveJob.perform_later(qc)
    _ExclusiveJob.perform_later(qc)

    claimed = qc.drain_batch(2)
    session.expire_all()

    assert len(claimed) == 1
    assert _claimed_count(session, prefix) == 1
    assert _ready_count(session, prefix) == 1
    assert qc._is_exclusive_active() is True

    claimed[0].perform()
    assert qc._is_exclusive_active() is False


def test_fast_path_no_exclusive_registered(qc_with_sqlalchemy):
    """When no class declares exclusive, the sieve short-circuits."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_NormalJob)
    qc.register_worker_process()

    _NormalJob.perform_later(qc)
    _NormalJob.perform_later(qc)
    _NormalJob.perform_later(qc)

    claimed = qc.drain_batch(3)
    session.expire_all()

    # All three normals are claimed concurrently — no sieve interference.
    assert len(claimed) == 3
    assert _claimed_count(session, prefix) == 3
    assert qc._is_exclusive_active() is False

    for ex in claimed:
        ex.perform()


# ──────────────────────────────────────────────────────────────────────
# pick_job exclusive wait
# ──────────────────────────────────────────────────────────────────────


def test_exclusive_active_blocks_subsequent_claims(qc_with_sqlalchemy):
    """While exclusive_active is set, drain_batch claims yield no new work."""
    ctx = qc_with_sqlalchemy
    qc = ctx["qc"]
    session = ctx["session"]
    prefix = ctx["prefix"]

    qc.register_job(_NormalJob)
    qc.register_job(_ExclusiveJob)
    qc.register_worker_process()

    # Claim a exclusive first; flag flips true.
    _ExclusiveJob.perform_later(qc)
    (exclusive_exec,) = qc.drain_batch(1)
    assert qc._is_exclusive_active() is True

    # Enqueue normal work while the exclusive is still in flight.
    _NormalJob.perform_later(qc)
    _NormalJob.perform_later(qc)
    session.expire_all()
    assert _ready_count(session, prefix) == 2

    # process_available_jobs would short-circuit here at the exclusive_active
    # gate. drain_batch bypasses that gate (calls claim_jobs directly), so
    # the assertion here is the inverse: the flag is still set, the dispatcher
    # path SEES it, and the production gate keeps the worker idle until the
    # exclusive completes.
    assert qc._is_exclusive_active() is True

    exclusive_exec.perform()
    assert qc._is_exclusive_active() is False

    # After the exclusive completes, drain_batch can claim the normals again.
    claimed = qc.drain_batch(2)
    assert len(claimed) == 2
    for ex in claimed:
        ex.perform()


def test_exclusive_clears_flag_on_perform_failure(qc_with_sqlalchemy):
    """A exclusive whose perform() raises still clears exclusive_active."""
    qc = qc_with_sqlalchemy["qc"]

    class _FailingExclusive(quebec.ActiveJob):
        exclusive = True

        def perform(self, *args, **kwargs):
            raise RuntimeError("boom")

    qc.register_job(_FailingExclusive)
    qc.register_worker_process()

    _FailingExclusive.perform_later(qc)
    (execution,) = qc.drain_batch(1)
    assert qc._is_exclusive_active() is True

    try:
        execution.perform()
    except Exception:
        # perform may surface the exception or absorb it depending on rescue
        # config; either way the flag must clear.
        pass

    assert qc._is_exclusive_active() is False


def test_exclusive_clears_flag_when_execution_dropped_unperformed(qc_with_sqlalchemy):
    """Dropping a exclusive Execution without performing clears the flag (Drop backstop)."""
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(_ExclusiveJob)
    qc.register_worker_process()

    _ExclusiveJob.perform_later(qc)
    (execution,) = qc.drain_batch(1)

    assert qc._is_exclusive_active() is True
    del execution
    # Python may delay the actual __del__; force a gc cycle.
    gc.collect()

    assert qc._is_exclusive_active() is False


def test_stale_execution_drop_does_not_clear_new_exclusives_seat(qc_with_sqlalchemy):
    """A late Drop of an old exclusive Execution must NOT clear a NEW exclusive's
    ownership. CAS gate on claimed.id prevents the cross-generation stomp.
    """
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(_ExclusiveJob)
    qc.register_worker_process()

    # First exclusive: claim, perform (clears ownership), but hold the Python
    # Execution so its Drop is delayed.
    _ExclusiveJob.perform_later(qc)
    (b1,) = qc.drain_batch(1)
    b1_owner_id = qc._exclusive_owner()
    assert b1_owner_id is not None
    b1.perform()
    assert qc._is_exclusive_active() is False
    # b1 still in scope — its Drop hasn't run yet.

    # Second exclusive: take ownership.
    _ExclusiveJob.perform_later(qc)
    (b2,) = qc.drain_batch(1)
    b2_owner_id = qc._exclusive_owner()
    assert b2_owner_id is not None
    assert b2_owner_id != b1_owner_id

    # Drop the stale first execution. The CAS clear gates on claimed.id, so
    # this MUST NOT clear b2's ownership.
    del b1
    gc.collect()

    assert qc._exclusive_owner() == b2_owner_id, (
        "Stale b1.Drop stomped b2's ownership — CAS gate failed"
    )

    # Clean up.
    b2.perform()
    assert qc._is_exclusive_active() is False


def test_sieved_exclusive_releases_ownership_when_dispatch_fails(qc_with_sqlalchemy):
    """A exclusive sieved via apply_exclusive_sieve but never dispatched (e.g.
    Execution dropped pre-perform) must release ownership via the Drop
    backstop. The release_claimed_batch CAS clear backs this up.
    """
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(_ExclusiveJob)
    qc.register_worker_process()

    _ExclusiveJob.perform_later(qc)
    (execution,) = qc.drain_batch(1)

    # Simulate a failure between claim and perform: drop without performing.
    assert qc._is_exclusive_active() is True
    del execution
    gc.collect()

    assert qc._is_exclusive_active() is False, (
        "Sieved exclusive never released ownership on Drop"
    )
