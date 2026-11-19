"""``preserve_finished_jobs`` decides whether a finished job is kept or deleted."""

from __future__ import annotations

import pytest
import quebec
from sqlalchemy import text

from .helpers import create_database_engine


class DoneJob(quebec.ActiveJob):
    queue_as = "default"

    def perform(self, *args, **kwargs):
        pass


class KeyedDoneJob(quebec.ActiveJob):
    queue_as = "default"
    concurrency_limit = 1

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "only-one"

    def perform(self, *args, **kwargs):
        pass


@pytest.fixture
def make_qc(db_url, test_prefix):
    instances = []

    def make(**kwargs):
        instance = quebec.Quebec(db_url, table_name_prefix=test_prefix, **kwargs)
        assert instance.create_tables() is True
        instances.append(instance)
        return instance

    yield make
    for instance in instances:
        instance.close()


def _count(db_url, prefix, table):
    engine = create_database_engine(db_url)
    try:
        with engine.connect() as conn:
            return conn.execute(text(f"SELECT COUNT(*) FROM {prefix}_{table}")).scalar()
    finally:
        engine.dispose()


def test_finished_job_is_kept_by_default(make_qc, db_url, test_prefix) -> None:
    qc = make_qc()
    qc.register_job(DoneJob)
    DoneJob.perform_later(qc)
    qc.drain_one().perform()

    assert _count(db_url, test_prefix, "jobs") == 1


def test_finished_job_is_deleted_without_preserve(make_qc, db_url, test_prefix) -> None:
    """Solid Queue's ``finished!``: without preserve_finished_jobs, destroy."""
    qc = make_qc(preserve_finished_jobs=False)
    qc.register_job(DoneJob)
    DoneJob.perform_later(qc)
    qc.drain_one().perform()

    assert _count(db_url, test_prefix, "jobs") == 0
    assert _count(db_url, test_prefix, "claimed_executions") == 0


def test_deleted_job_still_unblocks_the_next(make_qc, db_url, test_prefix) -> None:
    """The slot is given back from the job's data, after its row is gone."""
    qc = make_qc(preserve_finished_jobs=False)
    qc.register_job(KeyedDoneJob)
    KeyedDoneJob.perform_later(qc)
    KeyedDoneJob.perform_later(qc)
    assert _count(db_url, test_prefix, "blocked_executions") == 1

    qc.drain_one().perform()

    assert _count(db_url, test_prefix, "jobs") == 1
    assert _count(db_url, test_prefix, "blocked_executions") == 0
    assert _count(db_url, test_prefix, "ready_executions") == 1
