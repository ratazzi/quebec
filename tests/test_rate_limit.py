"""Tests for the experimental sliding-window rate limit feature.

Spec: docs/superpowers/specs/2026-05-23-experimental-rate-limit-sliding-window-design.md
"""

from __future__ import annotations

import time
from datetime import timedelta

import pytest
from sqlalchemy import text

import quebec


# Module-level job classes so __qualname__ matches their bare name —
# avoids the function-local `<locals>` prefix that would otherwise force
# tests to plumb qualname strings through `qc._rate_limit_config_for`.
class ThrottleJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 3
    rate_limit_duration = timedelta(seconds=2)
    calls: list[int] = []

    def perform(self, value: int) -> None:
        type(self).calls.append(value)


class SmallDurationJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 2
    rate_limit_duration = timedelta(seconds=1)
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


class DiscardJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 1
    rate_limit_duration = timedelta(seconds=2)
    rate_limit_on_throttle = quebec.RateLimitConflict.Discard
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


class StripeJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 1
    rate_limit_duration = timedelta(seconds=2)
    calls: list[tuple] = []

    def rate_limit_key(self, region: str = "us") -> str:
        return f"stripe:{region}"

    def perform(self, region: str = "us") -> None:
        type(self).calls.append(("call", region))


class PlainJob(quebec.BaseClass):
    """No rate limit declared; sanity test for zero-overhead path."""

    queue_as = "default"
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


def test_rate_limit_conflict_enum_exposed():
    assert hasattr(quebec, "RateLimitConflict")
    assert quebec.RateLimitConflict.Reschedule != quebec.RateLimitConflict.Discard
    assert repr(quebec.RateLimitConflict.Reschedule) == "RateLimitConflict.Reschedule"
    assert repr(quebec.RateLimitConflict.Discard) == "RateLimitConflict.Discard"


def test_register_job_extracts_rate_limit_config(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class ApiCall(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_duration = timedelta(seconds=2)
        rate_limit_on_throttle = quebec.RateLimitConflict.Discard

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(ApiCall)

    cfg = qc._rate_limit_config_for(ApiCall.__qualname__)
    assert cfg is not None
    assert cfg["max"] == 5
    assert cfg["duration_seconds"] == 2
    assert cfg["on_throttle"] == quebec.RateLimitConflict.Discard


def test_register_job_without_rate_limit_returns_none(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class PlainJob(quebec.BaseClass):
        def perform(self, *args, **kwargs):
            pass

    qc.register_job(PlainJob)
    assert qc._rate_limit_config_for(PlainJob.__qualname__) is None


def test_register_job_defaults_to_reschedule(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class DefaultThrottle(quebec.BaseClass):
        rate_limit_max = 10
        rate_limit_duration = timedelta(seconds=1)

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(DefaultThrottle)
    cfg = qc._rate_limit_config_for(DefaultThrottle.__qualname__)
    assert cfg["on_throttle"] == quebec.RateLimitConflict.Reschedule


def test_register_job_rejects_subsecond_duration(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class TooFast(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_duration = timedelta(milliseconds=500)

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_duration must be >= 1 second"):
        qc.register_job(TooFast)


def test_register_job_rejects_fractional_duration(qc_with_sqlalchemy):
    """1.9s would silently truncate to 1s under `as i32`, loosening the
    declared limit by ~50%. Refuse rather than guess floor/ceil/round.
    """
    qc = qc_with_sqlalchemy["qc"]

    class FractionalDuration(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_duration = timedelta(seconds=1, milliseconds=900)

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="whole number of seconds"):
        qc.register_job(FractionalDuration)


def test_register_job_rejects_max_without_duration(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class MissingDuration(quebec.BaseClass):
        rate_limit_max = 5

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_duration"):
        qc.register_job(MissingDuration)


def test_register_job_rejects_old_rate_limit_window_name(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class OldName(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_window = timedelta(seconds=1)

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_duration"):
        qc.register_job(OldName)


def test_default_rate_limit_key_is_class_name():
    class Foo(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_duration = timedelta(seconds=1)

        def perform(self, *args, **kwargs):
            pass

    assert Foo().rate_limit_key() == "Foo"


def test_rate_limit_key_can_be_overridden():
    class Bar(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_duration = timedelta(seconds=1)

        def rate_limit_key(self, region="us"):
            return f"bar:{region}"

        def perform(self, *args, **kwargs):
            pass

    assert Bar().rate_limit_key(region="eu") == "bar:eu"
    assert Bar().rate_limit_key() == "bar:us"


# ──────────────────────────────────────────────────────────────────────
# End-to-end claim-time gating
# ──────────────────────────────────────────────────────────────────────


def _drain_silently(qc) -> object | None:
    """Try `qc.drain_one()`; return None when no claimable job is left.

    Quebec raises a string-matched "No ready job to claim" exception when
    the claim path can't find anything — we want a Pythonic bool/None.
    """
    try:
        return qc.drain_one()
    except Exception as exc:
        if "No ready job to claim" in str(exc) or "No job found" in str(exc):
            return None
        raise


def _reset_call_state():
    """Reset module-level class state between tests."""
    ThrottleJob.calls = []
    SmallDurationJob.calls = []
    DiscardJob.calls = []
    StripeJob.calls = []
    PlainJob.calls = []


def test_throttle_after_max_in_one_window(qc_with_sqlalchemy):
    _reset_call_state()
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ThrottleJob)
    for i in range(5):
        ThrottleJob.perform_later(qc, i)

    # First three claims grant.
    granted = 0
    for _ in range(3):
        e = _drain_silently(qc)
        if e is not None:
            granted += 1
            e.perform()
    assert granted == 3
    assert ThrottleJob.calls == [0, 1, 2]

    # Subsequent claims throttle (jobs 4, 5 → moved to scheduled_executions).
    for _ in range(2):
        assert _drain_silently(qc) is None

    scheduled = session.execute(
        text(f"SELECT count(*) FROM {prefix}_scheduled_executions")
    ).scalar()
    assert scheduled == 2


def test_recovers_after_window_elapses(qc_with_sqlalchemy):
    _reset_call_state()
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(SmallDurationJob)
    for i in range(2):
        SmallDurationJob.perform_later(qc, i)

    # Two grants in first window.
    e1 = _drain_silently(qc)
    e2 = _drain_silently(qc)
    assert e1 is not None and e2 is not None
    e1.perform()
    e2.perform()

    # Third enqueue in same window → throttled into scheduled.
    SmallDurationJob.perform_later(qc, 99)
    assert _drain_silently(qc) is None
    scheduled = session.execute(
        text(f"SELECT count(*) FROM {prefix}_scheduled_executions")
    ).scalar()
    assert scheduled == 1

    # Wait for window roll + a little slack for the retry_at jitter
    # (window=1s, max jitter ≈ window → ≤2s in the worst case).
    time.sleep(2.5)

    # Move the matured scheduled row back into ready (mirrors what
    # dispatcher polling would do). Drive it via raw SQL to keep the test
    # self-contained.
    session.execute(
        text(
            f"INSERT INTO {prefix}_ready_executions "
            "(job_id, queue_name, priority, created_at) "
            f"SELECT job_id, queue_name, priority, scheduled_at "
            f"FROM {prefix}_scheduled_executions"
        )
    )
    session.execute(text(f"DELETE FROM {prefix}_scheduled_executions"))
    session.commit()

    e3 = _drain_silently(qc)
    assert e3 is not None
    e3.perform()
    assert SmallDurationJob.calls == [0, 1, 99]


def test_discard_mode_finishes_throttled_jobs(qc_with_sqlalchemy):
    _reset_call_state()
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(DiscardJob)
    DiscardJob.perform_later(qc, 1)
    DiscardJob.perform_later(qc, 2)

    e1 = _drain_silently(qc)
    assert e1 is not None
    e1.perform()

    # Second job throttled → marked finished, not rescheduled.
    assert _drain_silently(qc) is None

    finished = session.execute(
        text(f"SELECT count(*) FROM {prefix}_jobs WHERE finished_at IS NOT NULL")
    ).scalar()
    assert finished == 2  # one ran, one discarded

    scheduled = session.execute(
        text(f"SELECT count(*) FROM {prefix}_scheduled_executions")
    ).scalar()
    assert scheduled == 0

    assert DiscardJob.calls == [1]  # only the first ran


def test_buckets_isolated_by_user_key(qc_with_sqlalchemy):
    _reset_call_state()
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(StripeJob)
    StripeJob.perform_later(qc, region="us")
    StripeJob.perform_later(qc, region="eu")
    StripeJob.perform_later(qc, region="us")

    # First two land in different buckets → both grant.
    e1 = _drain_silently(qc)
    e2 = _drain_silently(qc)
    assert e1 is not None and e2 is not None
    e1.perform()
    e2.perform()

    # Third (region=us) hits the saturated us bucket → throttled.
    assert _drain_silently(qc) is None

    regions_run = sorted(r for (_, r) in StripeJob.calls)
    assert regions_run == ["eu", "us"]


def test_zero_overhead_plain_job_still_claims(qc_with_sqlalchemy):
    """Sanity: when no class has rate_limit_max, the gate is skipped and
    normal jobs claim without issue.

    The zero-overhead path itself is asserted by code review (single
    `is_empty()` check), not a runtime spy — instrumenting the jobs-table
    read would require Rust-side hooks beyond this PR's scope.
    """
    _reset_call_state()
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(PlainJob)
    PlainJob.perform_later(qc, 42)

    e = _drain_silently(qc)
    assert e is not None
    e.perform()
    assert PlainJob.calls == [42]


class BurstJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 5
    rate_limit_duration = timedelta(seconds=5)
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


def test_burst_respects_max_within_window(qc_with_sqlalchemy):
    """Enqueue many jobs in tight succession; assert the rate limit caps
    granted claims at ``max`` within the configured window.

    Note: SQLite serializes all transactions, so threaded multi-worker
    contention can't be exercised here — a sequential burst already
    surfaces the invariant. True multi-process / multi-worker stress
    testing is deferred to PG-backed integration runs during soak.
    """
    BurstJob.calls = []
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(BurstJob)
    for i in range(20):
        BurstJob.perform_later(qc, i)

    granted = 0
    for _ in range(20):
        e = _drain_silently(qc)
        if e is None:
            break
        granted += 1
        e.perform()

    # Sliding window allows up to ~2% over the configured max.
    assert granted <= BurstJob.rate_limit_max + 1, (
        f"granted {granted} exceeds max={BurstJob.rate_limit_max} + 2% tolerance"
    )
    assert granted >= BurstJob.rate_limit_max, (
        f"granted {granted} is below max={BurstJob.rate_limit_max}; rate gate too strict"
    )

    rescheduled = session.execute(
        text(f"SELECT count(*) FROM {prefix}_scheduled_executions")
    ).scalar()
    assert rescheduled == 20 - granted


class QueueRateJob(quebec.BaseClass):
    """Used in test_rate_throttle_releases_queue_slot."""

    queue_as = "default"
    rate_limit_max = 1
    rate_limit_duration = timedelta(seconds=2)
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


def test_rate_throttle_releases_queue_slot(db_url, test_prefix):
    """REVIEW.md 2026-05-23 14:12 P1: when both queue concurrency and rate
    limit are enabled, a rate-throttled job must release the queue:<name>
    semaphore slot it took during the earlier queue-acquire step. Otherwise
    the queue is stuck "full" until the TTL sweep, blocking unrelated jobs
    on the same queue.
    """
    QueueRateJob.calls = []
    instance = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        experimental_queue_concurrency={"default": 1},
    )
    assert instance.create_tables() is True
    try:
        instance.register_job(QueueRateJob)
        QueueRateJob.perform_later(instance, 1)
        QueueRateJob.perform_later(instance, 2)
        QueueRateJob.perform_later(instance, 3)

        # First drain consumes the only rate token AND the only queue slot.
        e1 = _drain_silently(instance)
        assert e1 is not None
        e1.perform()  # releases the queue slot via after_executed

        # Second drain: queue slot acquired (1/1), then rate gate denies
        # (token already consumed in window). The buggy implementation
        # would leak the queue slot here — subsequent perform_later jobs
        # to the same queue would silently sit in ready_executions until
        # TTL even though no claimed jobs hold the slot.
        assert _drain_silently(instance) is None

        # Third drain must still be possible (queue slot was released even
        # though the rate gate routed the second candidate). With the
        # bug present, this would also return None and the assertion
        # below on PlainJob would also fail.
        instance.register_job(PlainJob)
        PlainJob.calls = []
        PlainJob.perform_later(instance, 99)
        e3 = _drain_silently(instance)
        assert e3 is not None, (
            "queue:default semaphore leaked; PlainJob can't claim its slot"
        )
        e3.perform()
        assert PlainJob.calls == [99]
    finally:
        instance.close()


def test_reregister_without_rate_limit_clears_stale_config(qc_with_sqlalchemy):
    """REVIEW.md 2026-05-23 14:12 P3: re-registering a class without
    `rate_limit_max` must clear the previously-stored rate config — otherwise
    long-lived processes / test reuse keep throttling against stale settings.
    """
    qc = qc_with_sqlalchemy["qc"]

    class Reused(quebec.BaseClass):
        rate_limit_max = 1
        rate_limit_duration = timedelta(seconds=2)

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(Reused)
    assert qc._rate_limit_config_for(Reused.__qualname__) is not None

    # Re-define the same class name without rate limit and re-register.
    class Reused(quebec.BaseClass):  # noqa: F811 — intentional shadow
        def perform(self, *args, **kwargs):
            pass

    qc.register_job(Reused)
    assert qc._rate_limit_config_for(Reused.__qualname__) is None


class CombinedClassRateJob(quebec.BaseClass):
    """concurrency_limit=1 + rate_limit_max=1 + rate_limit_duration=2s.

    Used by test_rate_throttle_releases_concurrency_key_slot to exercise
    the interaction between class-level concurrency_key (acquired at
    enqueue) and rate gating (deciding at claim time).
    """

    queue_as = "default"
    concurrency_limit = 1
    rate_limit_max = 1
    rate_limit_duration = timedelta(seconds=2)
    calls: list[int] = []

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "combined-key"

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


def test_rate_throttle_releases_concurrency_key_slot(qc_with_sqlalchemy):
    """When a class has both concurrency_limit and rate_limit_max:

    - enqueue acquires concurrency slot
    - claim consults rate gate
    - if rate throttles (Reschedule or Discard), the concurrency slot must
      be released, otherwise:
       * Reschedule path: dispatcher promotes the scheduled job, tries to
         re-acquire the same key, fails (slot still held by the very job
         being promoted), routes it to blocked_executions — job stalls
         until concurrency_duration TTL (default 60s).
       * Discard path: slot stays held for an already-finished job until
         TTL.

    This test exercises the Reschedule path by enqueuing a follow-up job
    after a rate-throttle and asserting it can immediately acquire (proves
    the slot was freed).
    """
    CombinedClassRateJob.calls = []
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(CombinedClassRateJob)

    # job1: enqueue → acquire slot (used 1/1) → ready. Claim, perform,
    # after_executed releases the slot (used 0/1) and consumes 1 rate
    # token.
    CombinedClassRateJob.perform_later(qc, 1)
    e1 = _drain_silently(qc)
    assert e1 is not None
    e1.perform()

    # job2: enqueue inside the same rate window → re-acquire slot (1/1) →
    # ready. Claim consults rate gate; token is exhausted → Reschedule.
    # With the bug, the slot stays at 1/1 even though job2 left ready.
    # With the fix, the slot is released back to 0/1.
    CombinedClassRateJob.perform_later(qc, 2)
    assert _drain_silently(qc) is None  # rate-throttled, moved to scheduled

    # job3: enqueue → must successfully acquire (slot expected to be 0/1
    # under the fix) → land in ready_executions, NOT blocked_executions.
    # With the bug, acquire fails → job3 goes to blocked.
    CombinedClassRateJob.perform_later(qc, 3)

    ready_count = session.execute(
        text(
            f"SELECT count(*) FROM {prefix}_ready_executions e "
            f"JOIN {prefix}_jobs j ON j.id = e.job_id "
            "WHERE j.class_name = 'CombinedClassRateJob'"
        )
    ).scalar()
    blocked_count = session.execute(
        text(
            f"SELECT count(*) FROM {prefix}_blocked_executions e "
            f"JOIN {prefix}_jobs j ON j.id = e.job_id "
            "WHERE j.class_name = 'CombinedClassRateJob'"
        )
    ).scalar()
    assert ready_count == 1, (
        f"expected job3 in ready_executions; ready={ready_count}, "
        f"blocked={blocked_count} — concurrency slot leak suspected"
    )
    assert blocked_count == 0


class CombinedDiscardJob(quebec.BaseClass):
    queue_as = "default"
    concurrency_limit = 1
    rate_limit_max = 1
    rate_limit_duration = timedelta(seconds=2)
    rate_limit_on_throttle = quebec.RateLimitConflict.Discard
    calls: list[int] = []

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "discard-key"

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


def test_rate_discard_releases_concurrency_key_slot(qc_with_sqlalchemy):
    """Same intent as above but exercises the Discard branch."""
    CombinedDiscardJob.calls = []
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(CombinedDiscardJob)

    CombinedDiscardJob.perform_later(qc, 1)
    e1 = _drain_silently(qc)
    assert e1 is not None
    e1.perform()

    # job2 enters in-window → throttled, Discard mode marks it finished.
    # Without the fix, the concurrency slot stays held for the finished
    # job until TTL.
    CombinedDiscardJob.perform_later(qc, 2)
    assert _drain_silently(qc) is None

    # job3 must acquire the freshly-released slot and land in ready.
    CombinedDiscardJob.perform_later(qc, 3)
    ready_count = session.execute(
        text(
            f"SELECT count(*) FROM {prefix}_ready_executions e "
            f"JOIN {prefix}_jobs j ON j.id = e.job_id "
            "WHERE j.class_name = 'CombinedDiscardJob'"
        )
    ).scalar()
    blocked_count = session.execute(
        text(
            f"SELECT count(*) FROM {prefix}_blocked_executions e "
            f"JOIN {prefix}_jobs j ON j.id = e.job_id "
            "WHERE j.class_name = 'CombinedDiscardJob'"
        )
    ).scalar()
    assert ready_count == 1, (
        f"expected job3 in ready_executions; ready={ready_count}, "
        f"blocked={blocked_count} — concurrency slot leak (Discard path)"
    )
    assert blocked_count == 0


class JitterSpreadJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 1
    rate_limit_duration = timedelta(seconds=1)
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


def test_jitter_spreads_throttled_jobs_within_one_second_window(qc_with_sqlalchemy):
    """retry_at must use sub-second resolution. With integer-second jitter,
    every throttled job at window=1s landed on the exact same wall-clock
    second, defeating the anti-stampede design. After the fix, distinct
    job_ids produce distinct sub-second offsets.
    """
    JitterSpreadJob.calls = []
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(JitterSpreadJob)
    # First grant consumes the only token.
    JitterSpreadJob.perform_later(qc, 0)
    e1 = _drain_silently(qc)
    assert e1 is not None
    e1.perform()

    # Enqueue several more in the same window — all throttle and
    # reschedule. Without sub-second jitter their scheduled_at values
    # collapse onto one timestamp.
    for i in range(1, 8):
        JitterSpreadJob.perform_later(qc, i)
    for _ in range(7):
        assert _drain_silently(qc) is None

    rows = session.execute(
        text(
            f"SELECT s.scheduled_at FROM {prefix}_scheduled_executions s "
            f"JOIN {prefix}_jobs j ON j.id = s.job_id "
            "WHERE j.class_name = 'JitterSpreadJob' "
            "ORDER BY s.scheduled_at"
        )
    ).all()
    assert len(rows) == 7
    distinct_times = {row[0] for row in rows}
    # Allow some collision (job_id % 1000 hash) but require meaningful
    # spread — at minimum half the jobs land at unique sub-second offsets.
    assert len(distinct_times) >= 4, (
        f"jitter collapsed: {len(distinct_times)} distinct scheduled_at across "
        f"7 throttled jobs — expected sub-second spread. rows={rows}"
    )


class LargeWindowJob(quebec.BaseClass):
    """Maximum-supported window — exercises the jitter overflow boundary.

    Pre-fix formula `seed * window_secs * 1_000_000_000 / 1000` overflows
    i64 once `seed * window_secs > i64::MAX / 1e9 ≈ 9.22e9`. With
    `window_secs ≈ i32::MAX (~2.15e9)`, any `seed >= 5` overflows.
    `cargo` dev profile enables overflow-checks → panic in tests; release
    builds wrap silently → wildly wrong retry_at. Either way the fix
    (split into seconds + sub-second nanos before scaling) keeps every
    intermediate product within i64 across the full supported range.
    """

    queue_as = "default"
    rate_limit_max = 1
    # i32::MAX seconds — the upper bound accepted by register_job_class.
    rate_limit_duration = timedelta(seconds=2_147_483_647)
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


def test_large_window_jitter_no_overflow(qc_with_sqlalchemy):
    LargeWindowJob.calls = []
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(LargeWindowJob)

    # Burn through job ids 1..=4 with a granted-then-completed cycle (max=1
    # in a multi-decade window, so only the very first per-window grants).
    # We need a throttled job with seed >= 5 to trip the overflow boundary
    # at max window. Easiest path: enqueue several jobs and pick one with
    # an id >= 5; the rate-throttle will move it to scheduled.
    LargeWindowJob.perform_later(qc, 1)
    e1 = _drain_silently(qc)
    assert e1 is not None
    e1.perform()

    # Enqueue several more so subsequent throttles span seeds across the
    # overflow boundary (seed >= 5).
    for value in range(2, 8):
        LargeWindowJob.perform_later(qc, value)

    # All six are throttled and rescheduled. With the bug, jobs whose
    # id % 1000 >= 5 panic on overflow (dev profile) or wrap to a
    # nonsensical retry_at (release profile). With the fix, every call
    # produces a sane future timestamp.
    for _ in range(6):
        assert _drain_silently(qc) is None

    rows = session.execute(
        text(
            f"SELECT s.scheduled_at, s.job_id "
            f"FROM {prefix}_scheduled_executions s "
            f"JOIN {prefix}_jobs j ON j.id = s.job_id "
            "WHERE j.class_name = 'LargeWindowJob' "
            "ORDER BY s.job_id"
        )
    ).all()
    assert len(rows) == 6, rows

    from datetime import datetime, timezone, timedelta as td

    now = datetime.now(timezone.utc)
    # base scheduled_at is now + ~window_secs (since prev=0, retry_at jumps
    # to window_end). Plus jitter (0..window). Allow up to 2x window slack.
    upper = now + td(seconds=LargeWindowJob.rate_limit_duration.total_seconds() * 2)
    for scheduled_at, jid in rows:
        if isinstance(scheduled_at, str):
            scheduled_at = datetime.fromisoformat(scheduled_at)
        if scheduled_at.tzinfo is None:
            scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)
        delta = scheduled_at - now
        assert td(seconds=0) < delta < upper - now, (
            f"job {jid}: retry_at {scheduled_at} is implausible (delta={delta}); "
            "jitter overflow suspected"
        )


def test_register_job_rejects_non_timedelta_duration(qc_with_sqlalchemy):
    """Non-timedelta `rate_limit_duration` used to surface as a cryptic
    `AttributeError: 'int' object has no attribute 'total_seconds'`.
    The upfront is_instance check now reports the actual problem.
    """
    qc = qc_with_sqlalchemy["qc"]

    class BadDurationInt(quebec.BaseClass):
        rate_limit_max = 1
        rate_limit_duration = 2  # int, not timedelta

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(TypeError, match="must be a datetime.timedelta"):
        qc.register_job(BadDurationInt)

    class BadDurationStr(quebec.BaseClass):
        rate_limit_max = 1
        rate_limit_duration = "2 seconds"  # str

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(TypeError, match="must be a datetime.timedelta"):
        qc.register_job(BadDurationStr)


@pytest.mark.parametrize(
    "bad_return",
    [
        pytest.param("raise", id="raises"),
        pytest.param(None, id="returns_none"),
        pytest.param(42, id="returns_int"),
    ],
)
def test_rate_limit_key_failure_falls_back_to_class_name(
    qc_with_sqlalchemy, bad_return
):
    """A broken user `rate_limit_key` (raise / None / non-str) must not poison
    the claim loop. Surfacing the PyErr would roll claim_job back, leave the
    candidate in ready, and next poll would hit the same error.
    """
    qc = qc_with_sqlalchemy["qc"]

    class Job(quebec.BaseClass):
        rate_limit_max = 1
        rate_limit_duration = timedelta(seconds=2)

        def rate_limit_key(self, *args, **kwargs):
            if bad_return == "raise":
                raise RuntimeError("user bug")
            return bad_return

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(Job)
    Job.perform_later(qc)
    e1 = _drain_silently(qc)
    assert e1 is not None  # granted via fallback class-name bucket
    e1.perform()
    Job.perform_later(qc)
    # Throttled via the same fallback bucket — no crash, no loop.
    assert _drain_silently(qc) is None
