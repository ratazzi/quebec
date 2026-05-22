"""Tests for the experimental per-queue concurrency limit.

The override is enforced at worker claim time: when the queue is listed in
``experimental_queue_concurrency`` and the ``queue:<name>`` semaphore slot
is full, the candidate job stays in ``ready_executions`` and the next
worker poll picks it up again.
"""

from __future__ import annotations

import pytest

import quebec


class PlainJob(quebec.BaseClass):
    queue_as = "default"
    calls: list[int] = []

    def perform(self, value: int) -> None:
        type(self).calls.append(value)


class OtherJob(quebec.BaseClass):
    queue_as = "other"
    calls: list[int] = []

    def perform(self, value: int) -> None:
        type(self).calls.append(value)


def _qc(db_url, prefix, **overrides):
    instance = quebec.Quebec(db_url, table_name_prefix=prefix, **overrides)
    assert instance.create_tables() is True
    return instance


def test_queue_limit_blocks_concurrent_claim(db_url, test_prefix) -> None:
    """With ``experimental_queue_concurrency={"default": 1}``, claiming a
    job from "default" holds the slot. A second claim attempt against the
    same queue must fail until the first job releases.
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"default": 1})
    try:
        PlainJob.calls = []
        qc.register_job(PlainJob)

        PlainJob.perform_later(qc, 1)
        PlainJob.perform_later(qc, 2)

        # First drain acquires queue:default → succeeds.
        e1 = qc.drain_one()

        # While e1 is still pending perform(), queue:default is full.
        # Second drain must fail to claim.
        with pytest.raises(Exception, match="No ready job to claim"):
            qc.drain_one()

        # After e1 runs, queue slot frees → next drain works.
        e1.perform()
        e2 = qc.drain_one()
        e2.perform()
        assert PlainJob.calls == [1, 2]
    finally:
        qc.close()


def test_queue_limit_does_not_affect_other_queues(db_url, test_prefix) -> None:
    """A throttled queue must not block claims from other queues. Worker
    falls through to the next queue when one is throttled.
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"default": 1})
    try:
        PlainJob.calls = []
        OtherJob.calls = []
        qc.register_job(PlainJob)
        qc.register_job(OtherJob)

        PlainJob.perform_later(qc, 1)
        PlainJob.perform_later(qc, 2)
        OtherJob.perform_later(qc, 99)

        # First drain takes the default slot (alphabetical/insertion order
        # not guaranteed, but ready_executions FIFO by id → PlainJob 1 first).
        e1 = qc.drain_one()
        assert e1 is not None

        # Default is full now. Next drain should still find OtherJob in
        # "other" queue, unaffected by the default-queue throttle.
        e2 = qc.drain_one()
        e2.perform()

        # Default still throttled — third drain must fail.
        with pytest.raises(Exception, match="No ready job to claim"):
            qc.drain_one()

        # Release default.
        e1.perform()

        # Now PlainJob 2 can be drained.
        e3 = qc.drain_one()
        e3.perform()

        assert PlainJob.calls == [1, 2]
        assert OtherJob.calls == [99]
    finally:
        qc.close()


def test_no_override_keeps_existing_behavior(db_url, test_prefix) -> None:
    """Without ``experimental_queue_concurrency`` set, concurrent claims on
    the same queue still succeed (no queue-level throttling at all).
    """
    qc = _qc(db_url, test_prefix)
    try:
        PlainJob.calls = []
        qc.register_job(PlainJob)

        PlainJob.perform_later(qc, 1)
        PlainJob.perform_later(qc, 2)

        # Both can be claimed back-to-back, no queue throttle in play.
        e1 = qc.drain_one()
        e2 = qc.drain_one()

        e1.perform()
        e2.perform()
        assert sorted(PlainJob.calls) == [1, 2]
    finally:
        qc.close()


def test_batch_claim_respects_queue_limit(db_url, test_prefix) -> None:
    """Regression for P1 in the review: the production worker uses the
    batch ``claim_jobs`` path, not the single-job ``claim_job``. Tests that
    only exercise ``drain_one`` (which calls ``claim_job``) miss the batch
    bypass. This test directly drives ``claim_jobs`` via the
    ``drain_batch`` API and asserts that a queue limit of 1 lets exactly
    one row leave ready_executions even when many are available.
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"default": 1})
    try:
        PlainJob.calls = []
        qc.register_job(PlainJob)

        # Five jobs all on "default" — limit=1 must allow only 1 to claim.
        for i in range(5):
            PlainJob.perform_later(qc, i)

        claimed = qc.drain_batch(10)
        assert len(claimed) == 1, (
            f"queue:default limit=1 was bypassed by claim_jobs; "
            f"got {len(claimed)} claimed"
        )
    finally:
        qc.close()


def test_batch_claim_no_head_of_line_block(db_url, test_prefix) -> None:
    """Regression: a throttled queue at the head of the priority order
    must not block claims from other queues in the same poll, even when
    ``batch_size`` is smaller than the count of throttled rows ahead.

    Scenario: enqueue [default-1, default-2, other-1] (FIFO id order),
    ``experimental_queue_concurrency={"default": 1}``, ``batch_size=2``.
    Old behaviour: find_many returns [default-1, default-2]; default-1
    claims the slot, default-2 throttled, other-1 left in ready — to wait
    for next poll. Fixed behaviour: claim_jobs re-finds with `default` in
    the exclude list within the same transaction → other-1 also gets
    claimed.
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"default": 1})
    try:
        PlainJob.calls = []
        OtherJob.calls = []
        qc.register_job(PlainJob)
        qc.register_job(OtherJob)

        PlainJob.perform_later(qc, 1)
        PlainJob.perform_later(qc, 2)
        OtherJob.perform_later(qc, 100)

        # batch_size=2: prior implementation would only claim default-1.
        claimed = qc.drain_batch(2)
        assert len(claimed) == 2, (
            f"head-of-line stall: throttled queue blocked other queue in "
            f"the same poll; got {len(claimed)} claimed"
        )
    finally:
        qc.close()


def test_batch_claim_partitions_per_queue(db_url, test_prefix) -> None:
    """Same batch path, mixed queues: a throttled queue contributes at most
    its limit while unrestricted queues stay unaffected within the same
    batch call.
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"default": 1})
    try:
        PlainJob.calls = []
        OtherJob.calls = []
        qc.register_job(PlainJob)
        qc.register_job(OtherJob)

        # 3 jobs on throttled "default", 2 on unrestricted "other".
        for i in range(3):
            PlainJob.perform_later(qc, i)
        for i in range(2):
            OtherJob.perform_later(qc, 100 + i)

        claimed = qc.drain_batch(10)
        # 1 from default (limit=1) + 2 from other (no limit) = 3 total.
        assert len(claimed) == 3
    finally:
        qc.close()


def test_after_executed_releases_queue_slot(db_url, test_prefix) -> None:
    """Regression for P2 in the review: when a worker completes a job
    normally, the queue-level semaphore slot acquired at claim time must
    be released by ``after_executed``. Otherwise the slot stays held
    until the dispatcher's TTL sweep — falsely throttling later claims.

    Check: claim → assert slot held (next drain returns nothing) →
    perform() → assert slot freed (next drain succeeds).
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"default": 1})
    try:
        PlainJob.calls = []
        qc.register_job(PlainJob)

        PlainJob.perform_later(qc, 1)
        batch = qc.drain_batch(1)
        assert len(batch) == 1

        # Slot is held by the first claim — second drain must find nothing.
        assert qc.drain_batch(10) == []

        # perform() drives the shared `Worker::release_queue_slot` path.
        batch[0].perform()

        # Slot freed → next job claimable.
        PlainJob.perform_later(qc, 2)
        next_batch = qc.drain_batch(10)
        assert len(next_batch) == 1
        next_batch[0].perform()

        assert sorted(PlainJob.calls) == [1, 2]
    finally:
        qc.close()


def test_invalid_limit_is_ignored_with_warning(db_url, test_prefix) -> None:
    """Non-positive limits get dropped from the override map; the rest of
    the config still loads. (Validates the kwarg-parsing filter.)
    """
    qc = _qc(
        db_url,
        test_prefix,
        experimental_queue_concurrency={"default": 0, "": 5, "valid": 1},
    )
    try:
        PlainJob.calls = []
        qc.register_job(PlainJob)

        # default's entry was dropped (limit=0) → no throttle on default.
        PlainJob.perform_later(qc, 1)
        PlainJob.perform_later(qc, 2)

        # Both claim back-to-back since "default" wasn't actually registered.
        e1 = qc.drain_one()
        e2 = qc.drain_one()
        e1.perform()
        e2.perform()
        assert sorted(PlainJob.calls) == [1, 2]
    finally:
        qc.close()
