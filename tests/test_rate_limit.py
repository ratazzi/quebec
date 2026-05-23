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
    rate_limit_window = timedelta(seconds=2)
    calls: list[int] = []

    def perform(self, value: int) -> None:
        type(self).calls.append(value)


class SmallWindowJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 2
    rate_limit_window = timedelta(seconds=1)
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


class DiscardJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 1
    rate_limit_window = timedelta(seconds=2)
    rate_limit_on_throttle = quebec.RateLimitConflict.Discard
    calls: list[int] = []

    def perform(self, value: int = 0) -> None:
        type(self).calls.append(value)


class StripeJob(quebec.BaseClass):
    queue_as = "default"
    rate_limit_max = 1
    rate_limit_window = timedelta(seconds=2)
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
        rate_limit_window = timedelta(seconds=2)
        rate_limit_on_throttle = quebec.RateLimitConflict.Discard

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(ApiCall)

    cfg = qc._rate_limit_config_for(ApiCall.__qualname__)
    assert cfg is not None
    assert cfg["max"] == 5
    assert cfg["window_seconds"] == 2
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
        rate_limit_window = timedelta(seconds=1)

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(DefaultThrottle)
    cfg = qc._rate_limit_config_for(DefaultThrottle.__qualname__)
    assert cfg["on_throttle"] == quebec.RateLimitConflict.Reschedule


def test_register_job_rejects_subsecond_window(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class TooFast(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_window = timedelta(milliseconds=500)

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_window must be >= 1 second"):
        qc.register_job(TooFast)


def test_register_job_rejects_fractional_window(qc_with_sqlalchemy):
    """1.9s would silently truncate to 1s under `as i32`, loosening the
    declared limit by ~50%. Refuse rather than guess floor/ceil/round.
    """
    qc = qc_with_sqlalchemy["qc"]

    class FractionalWindow(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_window = timedelta(seconds=1, milliseconds=900)

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="whole number of seconds"):
        qc.register_job(FractionalWindow)


def test_register_job_rejects_max_without_window(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class MissingWindow(quebec.BaseClass):
        rate_limit_max = 5

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_window"):
        qc.register_job(MissingWindow)


def test_default_rate_limit_key_is_class_name():
    class Foo(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_window = timedelta(seconds=1)

        def perform(self, *args, **kwargs):
            pass

    assert Foo().rate_limit_key() == "Foo"


def test_rate_limit_key_can_be_overridden():
    class Bar(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_window = timedelta(seconds=1)

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
    SmallWindowJob.calls = []
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

    qc.register_job(SmallWindowJob)
    for i in range(2):
        SmallWindowJob.perform_later(qc, i)

    # Two grants in first window.
    e1 = _drain_silently(qc)
    e2 = _drain_silently(qc)
    assert e1 is not None and e2 is not None
    e1.perform()
    e2.perform()

    # Third enqueue in same window → throttled into scheduled.
    SmallWindowJob.perform_later(qc, 99)
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
    assert SmallWindowJob.calls == [0, 1, 99]


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
    rate_limit_window = timedelta(seconds=5)
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
    rate_limit_window = timedelta(seconds=2)
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
        rate_limit_window = timedelta(seconds=2)

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
