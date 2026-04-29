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

    assert len(inserted) == 3
    assert all(job.id is not None for job in inserted)
    assert [job.id for job in inserted] == [
        row.id
        for row in session.execute(
            text(f"SELECT id FROM {prefix}_jobs ORDER BY id")
        ).fetchall()
    ]
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


def test_perform_all_later_returns_active_jobs_in_input_order(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(BulkJob)

    descriptors = [BulkJob.build(i) for i in range(5)]
    inserted = qc.perform_all_later(descriptors)

    assert len(inserted) == 5
    assert all(isinstance(job, quebec.ActiveJob) for job in inserted)

    ids = [job.id for job in inserted]
    assert ids == sorted(ids)
    assert len(set(ids)) == 5

    for job in inserted:
        assert job.queue_name == "bulk-default"
        assert job.priority == 0
        assert job.active_job_id

    active_job_ids = [job.active_job_id for job in inserted]
    assert len(set(active_job_ids)) == 5


def test_perform_all_later_populates_overrides_on_returned_jobs(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(BulkJob)

    inserted = qc.perform_all_later(
        [
            BulkJob.build(1),
            BulkJob.set(queue="critical", priority=9).build(2),
            BulkJob.set(wait=timedelta(minutes=5)).build(3),
        ]
    )

    assert inserted[0].queue_name == "bulk-default"
    assert inserted[0].priority == 0

    assert inserted[1].queue_name == "critical"
    assert inserted[1].priority == 9

    assert inserted[2].queue_name == "bulk-default"
    # scheduled_at should reflect the wait offset (~5 minutes from now)
    delta = inserted[2].scheduled_at - inserted[0].scheduled_at
    assert delta >= timedelta(minutes=4, seconds=30)


def test_perform_all_later_returns_empty_list_for_empty_input(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(BulkJob)

    assert qc.perform_all_later([]) == []


class PythonicAliasJob(quebec.BaseClass):
    queue = "alias-q"
    priority = 11

    def perform(self, value: int) -> None:
        return None


class AliasOverridesSolidQueueJob(quebec.BaseClass):
    # Pythonic names take precedence when both are present.
    queue = "primary-q"
    queue_as = "fallback-q"
    priority = 21
    queue_with_priority = 99

    def perform(self, value: int) -> None:
        return None


def test_perform_later_reads_pythonic_queue_and_priority(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(PythonicAliasJob)

    enqueued = PythonicAliasJob.perform_later(qc, 5)
    persisted = get_job_by_active_job_id(session, prefix, enqueued.active_job_id)

    assert persisted["queue_name"] == "alias-q"
    assert persisted["priority"] == 11
    assert db_assert.count_jobs() == 1


def test_perform_later_prefers_pythonic_aliases_over_solid_queue_names(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    qc.register_job(AliasOverridesSolidQueueJob)

    enqueued = AliasOverridesSolidQueueJob.perform_later(qc, 7)
    persisted = get_job_by_active_job_id(session, prefix, enqueued.active_job_id)

    assert persisted["queue_name"] == "primary-q"
    assert persisted["priority"] == 21
    assert db_assert.count_jobs() == 1


class StringPriorityJob(quebec.BaseClass):
    priority = "10"  # type: ignore[assignment]

    def perform(self, value: int) -> None:
        return None


def test_register_job_raises_for_non_int_priority(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]

    import pytest

    with pytest.raises(
        TypeError, match=r"StringPriorityJob\.priority must be int, got str"
    ):
        qc.register_job(StringPriorityJob)


class NonStrQueueJob(quebec.BaseClass):
    queue = 100  # type: ignore[assignment]

    def perform(self, value: int) -> None:
        return None


def test_register_job_raises_for_non_str_queue(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]

    import pytest

    with pytest.raises(TypeError, match=r"NonStrQueueJob\.queue must be str, got int"):
        qc.register_job(NonStrQueueJob)
