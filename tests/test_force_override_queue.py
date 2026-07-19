"""QUEBEC_FORCE_OVERRIDE_QUEUE pins both enqueue and consumption.

README promises the override rewrites every enqueue to the forced queue AND
that the worker only consumes that queue, so branches sharing one database
never pick up each other's jobs.
"""

from __future__ import annotations

import pytest

import quebec


class OverrideJob(quebec.BaseClass):
    def perform(self, *args, **kwargs) -> None:
        pass


@pytest.fixture(autouse=True)
def _no_env_override(monkeypatch):
    # A developer using the feature may have the env var exported; these
    # tests control the override via kwargs only.
    monkeypatch.delenv("QUEBEC_FORCE_OVERRIDE_QUEUE", raising=False)


def test_enqueue_is_rewritten_to_forced_queue(temp_db_path, test_prefix) -> None:
    qc = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc",
        table_name_prefix=test_prefix,
        force_override_queue="pinned",
    )
    assert qc.create_tables() is True
    qc.register_job(OverrideJob)

    job = OverrideJob.set(queue="elsewhere").perform_later(qc)
    assert job.queue_name == "pinned"

    qc.close()


def test_wildcard_chars_are_sanitized_from_forced_queue(
    temp_db_path, test_prefix
) -> None:
    # A '*' in the forced name would be enqueued literally but reinterpreted
    # as a wildcard prefix on the consumption side, silently widening the
    # pinned worker to other branches' queues.
    qc = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc",
        table_name_prefix=test_prefix,
        force_override_queue="branch*",
    )
    assert qc.create_tables() is True
    qc.register_job(OverrideJob)

    job = OverrideJob.perform_later(qc)
    assert job.queue_name == "branch-"

    qc.close()


def test_worker_only_consumes_forced_queue(temp_db_path, test_prefix) -> None:
    dsn = f"sqlite:///{temp_db_path}?mode=rwc"

    # Another branch (no override) enqueues into its own queue.
    plain = quebec.Quebec(dsn, table_name_prefix=test_prefix)
    assert plain.create_tables() is True
    plain.register_job(OverrideJob)
    OverrideJob.set(queue="other_branch").perform_later(plain)

    # This branch runs with the override active.
    pinned = quebec.Quebec(
        dsn, table_name_prefix=test_prefix, force_override_queue="pinned"
    )
    pinned.register_job(OverrideJob)
    OverrideJob.perform_later(pinned)

    execution = pinned.drain_one()
    assert execution.queue == "pinned"
    execution.perform()

    # The other branch's job must not be claimable by the pinned worker.
    with pytest.raises(RuntimeError):
        pinned.drain_one()

    # The plain worker still sees it.
    execution = plain.drain_one()
    assert execution.queue == "other_branch"
    execution.perform()

    pinned.close()
    plain.close()


def test_worker_batch_only_consumes_forced_queue(temp_db_path, test_prefix) -> None:
    dsn = f"sqlite:///{temp_db_path}?mode=rwc"

    plain = quebec.Quebec(dsn, table_name_prefix=test_prefix)
    assert plain.create_tables() is True
    plain.register_job(OverrideJob)
    OverrideJob.set(queue="other_branch").perform_later(plain)

    pinned = quebec.Quebec(
        dsn, table_name_prefix=test_prefix, force_override_queue="pinned"
    )
    pinned.register_job(OverrideJob)
    OverrideJob.perform_later(pinned)

    executions = pinned.drain_batch(10)
    assert [execution.queue for execution in executions] == ["pinned"]
    executions[0].perform()

    assert pinned.drain_batch(10) == []

    execution = plain.drain_one()
    assert execution.queue == "other_branch"
    execution.perform()

    pinned.close()
    plain.close()
