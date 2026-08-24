"""Tests for the experimental `experimental_global_priority` flag.

A `*` worker that has to skip a queue — paused, or with a full
`experimental_queue_concurrency` slot — cannot express its poll as a single
unfiltered query. By default Quebec matches Solid Queue and degrades into one
query per queue, which makes queue order override `priority`. With the flag on,
the poll becomes `queue_name IN (live) ORDER BY priority, job_id` instead, so
one global priority order survives the exclusion.

Queue names below deliberately sort the opposite way to their priority, so
queue-order and priority-order are distinguishable from the claim sequence
alone.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

import quebec


class PriorityJob(quebec.BaseClass):
    calls: list[int] = []

    def perform(self, value: int) -> None:
        type(self).calls.append(value)


def _qc(db_url, prefix, **overrides):
    overrides.setdefault("experimental_global_priority", True)
    instance = quebec.Quebec(db_url, table_name_prefix=prefix, **overrides)
    assert instance.create_tables() is True
    PriorityJob.calls = []
    instance.register_job(PriorityJob)
    return instance


def _seed_inverted(qc):
    # a_low sorts first by queue name but last by priority.
    PriorityJob.set(queue="a_low", priority=100).perform_later(qc, 1)
    PriorityJob.set(queue="z_high", priority=-100).perform_later(qc, 2)


def test_off_by_default_matches_solid_queue(db_url, test_prefix) -> None:
    """Without the flag, a paused queue degrades the poll into per-queue
    queries and queue order takes precedence — Solid Queue's behaviour.
    """
    qc = _qc(db_url, test_prefix, experimental_global_priority=False)
    try:
        _seed_inverted(qc)
        assert qc.pause_queue("some_other_queue") is True

        first = qc.drain_one()
        assert first.queue == "a_low"
        first.perform()
    finally:
        qc.close()


def test_paused_queue_keeps_global_priority_single(db_url, test_prefix) -> None:
    qc = _qc(db_url, test_prefix)
    try:
        _seed_inverted(qc)
        # Pausing anything forces the `*` poll to stop being unfiltered.
        assert qc.pause_queue("some_other_queue") is True

        first = qc.drain_one()
        assert first.queue == "z_high", (
            f"expected the higher-priority queue first, got {first.queue!r}"
        )
        first.perform()

        second = qc.drain_one()
        assert second.queue == "a_low"
        second.perform()
    finally:
        qc.close()


def test_paused_queue_keeps_global_priority_batch(db_url, test_prefix) -> None:
    qc = _qc(db_url, test_prefix)
    try:
        _seed_inverted(qc)
        assert qc.pause_queue("some_other_queue") is True

        claimed = qc.drain_batch(10)
        assert [e.queue for e in claimed] == ["z_high", "a_low"]
        for execution in claimed:
            execution.perform()
    finally:
        qc.close()


def test_env_var_opts_in(db_url, test_prefix, monkeypatch) -> None:
    monkeypatch.setenv("QUEBEC_EXPERIMENTAL_GLOBAL_PRIORITY", "true")

    # No kwarg here: the env var alone must switch the behaviour on.
    instance = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert instance.create_tables() is True
        PriorityJob.calls = []
        instance.register_job(PriorityJob)

        _seed_inverted(instance)
        assert instance.pause_queue("some_other_queue") is True

        first = instance.drain_one()
        assert first.queue == "z_high", "env var opt-in did not take effect"
        first.perform()
    finally:
        instance.close()


def test_no_exclusion_keeps_single_poll(db_url, test_prefix) -> None:
    """Nothing to skip means the poll stays unfiltered, which already orders by
    priority — the flag changes nothing here.
    """
    qc = _qc(db_url, test_prefix)
    try:
        _seed_inverted(qc)

        first = qc.drain_one()
        assert first.queue == "z_high"
        first.perform()
    finally:
        qc.close()


def test_throttled_queue_keeps_global_priority(db_url, test_prefix) -> None:
    """The other trigger for exclusion: a full concurrency slot. The survivors
    must still be ordered by priority rather than by queue name.
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"throttled": 1})
    try:
        # `throttled` holds the top priority, so it is claimed first and its
        # slot is then full.
        PriorityJob.set(queue="throttled", priority=-200).perform_later(qc, 0)
        _seed_inverted(qc)

        held = qc.drain_one()
        assert held.queue == "throttled"

        second = qc.drain_one()
        assert second.queue == "z_high", (
            f"exclusion after a throttle lost priority order, got {second.queue!r}"
        )

        held.perform()
        second.perform()
    finally:
        qc.close()


def test_throttled_queue_is_dropped_from_the_list_not_excluded(
    db_url, test_prefix
) -> None:
    """A queue found throttled mid-poll is removed from the `IN` list and the
    poll retried, so the rest of the batch still fills from other queues.
    """
    qc = _qc(db_url, test_prefix, experimental_queue_concurrency={"throttled": 1})
    try:
        # Three throttled rows sit at the head of the priority order; only one
        # can be claimed. The batch must still reach `plain`.
        for i in range(3):
            PriorityJob.set(queue="throttled", priority=-100).perform_later(qc, i)
        for i in range(2):
            PriorityJob.set(queue="plain", priority=0).perform_later(qc, 100 + i)

        assert qc.pause_queue("some_other_queue") is True

        claimed = qc.drain_batch(3)
        queues = [e.queue for e in claimed]
        assert queues == ["throttled", "plain", "plain"], (
            f"throttled queue blocked the rest of the batch: {queues}"
        )
        for execution in claimed:
            execution.perform()
    finally:
        qc.close()


def test_batch_retries_after_rate_limit_routes_a_candidate(db_url, test_prefix) -> None:
    """Rows routed away by the rate limiter free up batch slots; the poll must
    be retried so those slots are filled from what is left.
    """

    class LimitedJob(quebec.BaseClass):
        queue_as = "limited"
        rate_limit_max = 1
        rate_limit_duration = timedelta(hours=1)

        def perform(self, _value: int) -> None:
            pass

    qc = _qc(db_url, test_prefix)
    try:
        qc.register_job(LimitedJob)

        # The first page is all rate-limited rows: one passes, one is routed.
        LimitedJob.set(priority=-100).perform_later(qc, 1)
        LimitedJob.set(priority=-100).perform_later(qc, 2)
        PriorityJob.set(queue="plain", priority=0).perform_later(qc, 3)

        assert qc.pause_queue("some_other_queue") is True

        claimed = qc.drain_batch(2)
        queues = [e.queue for e in claimed]
        assert queues == ["limited", "plain"], (
            f"batch came back short after a routed candidate: {queues}"
        )
        for execution in claimed:
            execution.perform()
    finally:
        qc.close()


@pytest.fixture
def ordered_queue_qc(tmp_path, monkeypatch, temp_db_path, test_prefix):
    queue_yml = tmp_path / "queue.yml"
    queue_yml.write_text(
        """
development:
  workers:
    - queues:
        - a_low
        - z_high
      threads: 1
"""
    )
    monkeypatch.setenv("QUEBEC_CONFIG", str(queue_yml))
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    instance = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc",
        table_name_prefix=test_prefix,
        experimental_global_priority=True,
    )
    assert instance.create_tables() is True
    PriorityJob.calls = []
    instance.register_job(PriorityJob)

    yield instance

    instance.close()


def test_explicit_queue_list_still_wins_over_priority(ordered_queue_qc) -> None:
    """Solid Queue's contract for an explicit list: queue order takes
    precedence over priority. The flag only ever applies to `*`.
    """
    _seed_inverted(ordered_queue_qc)

    first = ordered_queue_qc.drain_one()
    assert first.queue == "a_low", (
        "an explicit queue list must keep queue order ahead of priority"
    )
    first.perform()


@pytest.fixture
def wildcard_qc(tmp_path, monkeypatch, temp_db_path, test_prefix):
    queue_yml = tmp_path / "queue.yml"
    queue_yml.write_text(
        """
development:
  workers:
    - queues: "beta_*"
      threads: 1
"""
    )
    monkeypatch.setenv("QUEBEC_CONFIG", str(queue_yml))
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    instance = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc",
        table_name_prefix=test_prefix,
        experimental_global_priority=True,
    )
    assert instance.create_tables() is True
    PriorityJob.calls = []
    instance.register_job(PriorityJob)

    yield instance

    instance.close()


def test_wildcard_prefix_keeps_queue_order(wildcard_qc) -> None:
    """Wildcard prefixes expand to a queue list, which carries the same
    queue-order-first contract as an explicit list. The flag must not reach it.
    """
    PriorityJob.set(queue="beta_a", priority=100).perform_later(wildcard_qc, 1)
    PriorityJob.set(queue="beta_z", priority=-100).perform_later(wildcard_qc, 2)
    assert wildcard_qc.pause_queue("some_other_queue") is True

    first = wildcard_qc.drain_one()
    assert first.queue == "beta_a", (
        "wildcard expansion must keep queue order ahead of priority"
    )
    first.perform()
