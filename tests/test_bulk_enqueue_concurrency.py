"""Bulk enqueue keeps concurrency decisions and batch membership intact."""

import quebec
from sqlalchemy import text


class UniqueKeyJob(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(value: int) -> str:
        return f"bulk-{value}"

    def perform(self, value: int) -> None:
        pass


class PlainJob(quebec.BaseClass):
    def perform(self, value: int) -> None:
        pass


class SharedKeyJob(quebec.BaseClass):
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(value: int) -> str:
        return "shared-bulk"

    def perform(self, value: int) -> None:
        pass


class DiscardSharedKeyJob(quebec.BaseClass):
    concurrency_limit = 1
    concurrency_on_conflict = quebec.ConcurrencyConflict.Discard

    @staticmethod
    def concurrency_key(value: int) -> str:
        return "shared-bulk"

    def perform(self, value: int) -> None:
        pass


def test_bulk_mixed_destinations_keep_batch_membership(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    prefix = qc_with_sqlalchemy["prefix"]
    for job_class in (SharedKeyJob, DiscardSharedKeyJob, PlainJob):
        qc.register_job(job_class)

    with qc.batch() as batch:
        qc.perform_all_later(
            [
                SharedKeyJob.build(1),
                SharedKeyJob.build(2),
                DiscardSharedKeyJob.build(3),
                DiscardSharedKeyJob.build(4),
                PlainJob.build(5),
            ]
        )

    assert batch.reload().total_jobs == 5
    assert db_assert.count_jobs() == 5
    assert db_assert.count_ready_executions() == 3
    assert db_assert.count_blocked_executions() == 1
    assert db_assert.count_scheduled_executions() == 0
    with qc_with_sqlalchemy["engine"].connect() as observer:
        finished = observer.execute(
            text(f"SELECT COUNT(*) FROM {prefix}_jobs WHERE finished_at IS NOT NULL")
        ).scalar_one()
        membership = observer.execute(
            text(f"SELECT COUNT(*) FROM {prefix}_batch_executions")
        ).scalar_one()
    assert finished == 1
    assert membership == 4


def test_bulk_unique_keys_and_plain_jobs_share_one_batch(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    prefix = qc_with_sqlalchemy["prefix"]
    qc.register_job(UniqueKeyJob)
    qc.register_job(PlainJob)

    with qc.batch() as batch:
        qc.perform_all_later(
            [UniqueKeyJob.build(index) for index in range(32)]
            + [PlainJob.build(index) for index in range(32)]
        )

    assert batch.reload().total_jobs == 64
    assert db_assert.count_jobs() == 64
    assert db_assert.count_ready_executions() == 64
    assert db_assert.count_blocked_executions() == 0
    assert db_assert.count_scheduled_executions() == 0
    with qc_with_sqlalchemy["engine"].connect() as observer:
        semaphore_count = observer.execute(
            text(f"SELECT COUNT(*) FROM {prefix}_semaphores WHERE value = 0")
        ).scalar_one()
        membership_count = observer.execute(
            text(f"SELECT COUNT(*) FROM {prefix}_batch_executions")
        ).scalar_one()
    assert semaphore_count == 32
    assert membership_count == 64


def test_bulk_unique_keys_cross_ready_insert_chunk_boundary(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    prefix = qc_with_sqlalchemy["prefix"]
    qc.register_job(UniqueKeyJob)

    # SQLite's four-column ready INSERT is chunked at 249 rows. A 300-job
    # batch exercises that boundary while preserving each semaphore decision.
    qc.perform_all_later([UniqueKeyJob.build(index) for index in range(300)])

    assert db_assert.count_jobs() == 300
    assert db_assert.count_ready_executions() == 300
    assert db_assert.count_blocked_executions() == 0
    with qc_with_sqlalchemy["engine"].connect() as observer:
        semaphore_count = observer.execute(
            text(f"SELECT COUNT(*) FROM {prefix}_semaphores WHERE value = 0")
        ).scalar_one()
        ready_timestamps = observer.execute(
            text(f"SELECT COUNT(DISTINCT created_at) FROM {prefix}_ready_executions")
        ).scalar_one()
    assert semaphore_count == 300
    assert ready_timestamps > 1


def test_bulk_shared_key_crosses_blocked_insert_chunk_boundary(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    prefix = qc_with_sqlalchemy["prefix"]
    qc.register_job(SharedKeyJob)

    # SQLite's six-column blocked INSERT must be split beyond 166 rows.
    qc.perform_all_later([SharedKeyJob.build(index) for index in range(300)])

    assert db_assert.count_jobs() == 300
    assert db_assert.count_ready_executions() == 1
    assert db_assert.count_blocked_executions() == 299
    with qc_with_sqlalchemy["engine"].connect() as observer:
        value = observer.execute(
            text(f"SELECT value FROM {prefix}_semaphores")
        ).scalar_one()
    assert value == 0
