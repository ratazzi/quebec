from __future__ import annotations

import json
from datetime import timedelta

import quebec
from sqlalchemy import text

from .helpers import get_job_by_active_job_id


class DefaultQueueJob(quebec.BaseClass):
    def perform(self, name: str, **kwargs) -> None:
        return None


class ConfiguredJob(quebec.BaseClass):
    queue_as = "bulk"
    queue_with_priority = 7

    def perform(self, value: int) -> None:
        return None


class DynamicQueueJob(quebec.BaseClass):
    @staticmethod
    def queue_as(region: str, **kwargs) -> str:
        return f"{region}-{kwargs['kind']}"

    def perform(self, region: str, **kwargs) -> None:
        return None


class HookedJob(quebec.BaseClass):
    before_calls = 0
    after_calls = 0

    def before_enqueue(self) -> None:
        type(self).before_calls += 1

    def after_enqueue(self) -> None:
        type(self).after_calls += 1

    def perform(self, value: int) -> None:
        return None


class SkippedJob(quebec.BaseClass):
    before_calls = 0

    def before_enqueue(self) -> None:
        type(self).before_calls += 1
        raise quebec.AbortEnqueue()

    def perform(self, value: int) -> None:
        return None


class BulkJob(quebec.BaseClass):
    queue_as = "bulk-default"

    def perform(self, value: int, **kwargs) -> None:
        return None


def test_perform_later_persists_job_and_ready_execution(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(DefaultQueueJob)

    enqueued = DefaultQueueJob.perform_later(qc, "alice", flag=True)
    persisted = get_job_by_active_job_id(session, prefix, enqueued.active_job_id)
    payload = json.loads(persisted["arguments"])

    assert persisted["queue_name"] == "default"
    assert persisted["class_name"] == "DefaultQueueJob"
    assert persisted["priority"] == 0
    assert persisted["finished_at"] is None
    assert payload["job_class"] == "DefaultQueueJob"
    assert payload["queue_name"] == "default"
    assert payload["priority"] == 0
    assert payload["arguments"]["arguments"] == [
        "alice",
        {"flag": True, "_quebec_kwargs": True},
    ]
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 1


def test_job_builder_persists_queue_priority_and_schedule(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(ConfiguredJob)

    enqueued = ConfiguredJob.set(
        wait=timedelta(minutes=5), queue="critical", priority=2
    ).perform_later(
        qc,
        42,
    )
    persisted = get_job_by_active_job_id(session, prefix, enqueued.active_job_id)
    payload = json.loads(persisted["arguments"])

    assert persisted["queue_name"] == "critical"
    assert persisted["priority"] == 2
    assert persisted["scheduled_at"] is not None
    assert payload["queue_name"] == "critical"
    assert payload["priority"] == 2
    assert payload["arguments"]["arguments"] == [42]
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 0
    assert db_assert.count_scheduled_executions() == 1


def test_callable_queue_as_uses_filtered_kwargs(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(DynamicQueueJob)

    enqueued = DynamicQueueJob.set(
        queue="ignored-by-override", priority=9
    ).perform_later(
        qc,
        "eu",
        kind="imports",
    )
    persisted = get_job_by_active_job_id(session, prefix, enqueued.active_job_id)
    payload = json.loads(persisted["arguments"])

    assert persisted["queue_name"] == "ignored-by-override"
    assert persisted["priority"] == 9
    assert payload["arguments"]["arguments"] == [
        "eu",
        {"kind": "imports", "_quebec_kwargs": True},
    ]
    assert db_assert.count_jobs() == 1


def test_enqueue_hooks_are_invoked_for_regular_enqueue(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    HookedJob.before_calls = 0
    HookedJob.after_calls = 0
    qc.register_job(HookedJob)

    enqueued = HookedJob.perform_later(qc, 7)

    assert enqueued.id is not None
    assert HookedJob.before_calls == 1
    assert HookedJob.after_calls == 1
    assert db_assert.count_jobs() == 1
    assert db_assert.count_ready_executions() == 1


def test_abort_enqueue_skips_persistence(qc_with_sqlalchemy, db_assert) -> None:
    qc = qc_with_sqlalchemy["qc"]

    SkippedJob.before_calls = 0
    qc.register_job(SkippedJob)

    enqueued = SkippedJob.perform_later(qc, 99)

    assert SkippedJob.before_calls == 1
    assert enqueued.id is None
    assert db_assert.count_jobs() == 0
    assert db_assert.count_ready_executions() == 0


def test_perform_all_later_enqueues_multiple_descriptors(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(BulkJob)

    jobs = [
        BulkJob.build(1),
        BulkJob.set(queue="critical", priority=2).build(2, source="api"),
        BulkJob.set(wait=timedelta(minutes=1)).build(3),
    ]

    inserted = qc.perform_all_later(jobs)

    rows = session.execute(
        text(
            f"SELECT queue_name, priority, active_job_id, arguments, scheduled_at FROM {prefix}_jobs ORDER BY id"
        )
    ).fetchall()

    assert inserted == 3
    assert len(rows) == 3
    assert rows[0].queue_name == "bulk-default"
    assert rows[0].priority == 0
    assert json.loads(rows[0].arguments)["arguments"] == [1]
    assert rows[1].queue_name == "critical"
    assert rows[1].priority == 2
    assert json.loads(rows[1].arguments)["arguments"] == [
        2,
        {"source": "api", "_quebec_kwargs": True},
    ]
    assert rows[2].scheduled_at is not None
    assert db_assert.count_jobs() == 3
    assert db_assert.count_ready_executions() == 2
    assert db_assert.count_scheduled_executions() == 1
