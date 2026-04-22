# coding: utf-8
"""
Integration tests for Quebec.

These tests verify end-to-end functionality by checking database state
after operations. Each test uses a unique table prefix for isolation.
"""

import pytest
import quebec
from sqlalchemy import text


def test_baseclass_inherit():
    type("SubClass1", (quebec.BaseClass,), {})


def test_baseclass_override_init_raises():
    with pytest.raises(TypeError, match="is not allowed"):
        type("SubClass2", (quebec.BaseClass,), {"__init__": lambda self: None})


class TestJobEnqueue:
    """Test job enqueueing and database state."""

    def test_perform_later_creates_job_and_ready_execution(
        self, qc_with_sqlalchemy, fake_job_class
    ):
        """Enqueuing a job should create records in jobs and ready_executions tables."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        qc.register_job(fake_job_class)

        # Enqueue a job
        job = fake_job_class.perform_later(qc, "arg1", "arg2", key="value")

        # Verify job record
        result = session.execute(
            text(f"SELECT * FROM {prefix}_jobs WHERE id = :id"), {"id": job.id}
        )
        job_row = result.fetchone()
        assert job_row is not None
        job_data = dict(job_row._mapping)
        # Class name includes the full qualified name
        assert "FakeJob" in job_data["class_name"]
        assert job_data["queue_name"] == "default"
        assert job_data["finished_at"] is None

        # Verify ready execution record
        result = session.execute(
            text(f"SELECT * FROM {prefix}_ready_executions WHERE job_id = :job_id"),
            {"job_id": job.id},
        )
        ready_row = result.fetchone()
        assert ready_row is not None

    def test_perform_later_with_custom_queue(self, qc_with_sqlalchemy):
        """Job with queue_as should be placed in the specified queue."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class CriticalJob(quebec.ActiveJob):
            queue_as = "critical"

            def perform(self, *args, **kwargs):
                pass

        qc.register_job(CriticalJob)
        job = CriticalJob.perform_later(qc, "data")

        result = session.execute(
            text(f"SELECT queue_name FROM {prefix}_jobs WHERE id = :id"), {"id": job.id}
        )
        assert result.scalar() == "critical"

    def test_perform_later_with_priority(self, qc_with_sqlalchemy):
        """Job with queue_with_priority should have correct priority."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class HighPriorityJob(quebec.ActiveJob):
            queue_with_priority = 100

            def perform(self, *args, **kwargs):
                pass

        qc.register_job(HighPriorityJob)
        job = HighPriorityJob.perform_later(qc, "data")

        result = session.execute(
            text(f"SELECT priority FROM {prefix}_jobs WHERE id = :id"), {"id": job.id}
        )
        assert result.scalar() == 100


class TestConcurrency:
    """Test concurrency control via semaphores."""

    def test_job_with_concurrency_limit_acquires_semaphore(self, qc_with_sqlalchemy):
        """Job with concurrency limit should create semaphore record."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class LimitedJob(quebec.ActiveJob):
            concurrency_limit = 2

            def concurrency_key(self, *args, **kwargs):
                return "limited_resource"

            def perform(self, *args, **kwargs):
                pass

        qc.register_job(LimitedJob)
        LimitedJob.perform_later(qc, "data")

        # Check semaphore was created (key format: class_name/key)
        result = session.execute(
            text(f"SELECT * FROM {prefix}_semaphores WHERE key LIKE :key"),
            {"key": "%/limited_resource"},
        )
        semaphore = result.fetchone()
        assert semaphore is not None
        sem_data = dict(semaphore._mapping)
        # With limit=2, first job sets value to limit-1=1
        assert sem_data["value"] == 1

    def test_jobs_beyond_limit_are_blocked(self, qc_with_sqlalchemy):
        """Jobs exceeding concurrency limit should be blocked."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class SingletonJob(quebec.ActiveJob):
            concurrency_limit = 1

            def concurrency_key(self, *args, **kwargs):
                return "singleton_resource"

            def perform(self, *args, **kwargs):
                pass

        qc.register_job(SingletonJob)

        # Enqueue two jobs
        job1 = SingletonJob.perform_later(qc, "first")
        job2 = SingletonJob.perform_later(qc, "second")

        # First job should be in ready queue
        result = session.execute(
            text(
                f"SELECT COUNT(*) FROM {prefix}_ready_executions WHERE job_id = :job_id"
            ),
            {"job_id": job1.id},
        )
        assert result.scalar() == 1

        # Second job should be blocked
        result = session.execute(
            text(f"SELECT * FROM {prefix}_blocked_executions WHERE job_id = :job_id"),
            {"job_id": job2.id},
        )
        blocked = result.fetchone()
        assert blocked is not None
        blocked_data = dict(blocked._mapping)
        # Key format: class_name/key
        assert "singleton_resource" in blocked_data["concurrency_key"]


class TestTablePrefix:
    """Test dynamic table prefix functionality."""

    def test_custom_prefix_creates_correct_tables(self, db_url):
        """Tables should be created with the specified prefix."""
        qc = quebec.Quebec(db_url, table_name_prefix="myapp")
        qc.create_tables()

        # The tables should be named myapp_jobs, myapp_ready_executions, etc.
        # We can verify by trying to query them
        from sqlalchemy import create_engine

        sa_url = db_url.split("?")[0] if db_url.startswith("sqlite:") else db_url

        engine = create_engine(sa_url)
        with engine.connect() as conn:
            # This should work if tables exist
            result = conn.execute(
                text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'myapp_%'"
                )
            )
            tables = [row[0] for row in result]

            expected_tables = [
                "myapp_jobs",
                "myapp_ready_executions",
                "myapp_claimed_executions",
                "myapp_blocked_executions",
                "myapp_scheduled_executions",
                "myapp_failed_executions",
                "myapp_semaphores",
                "myapp_processes",
                "myapp_pauses",
                "myapp_recurring_tasks",
                "myapp_recurring_executions",
            ]

            for table in expected_tables:
                assert table in tables, f"Table {table} not found"

        engine.dispose()
        qc.close()

    def test_multiple_prefixes_isolated(self, db_url):
        """Different prefixes should create separate table sets."""
        qc1 = quebec.Quebec(db_url, table_name_prefix="app1")
        qc1.create_tables()

        qc2 = quebec.Quebec(db_url, table_name_prefix="app2")
        qc2.create_tables()

        class Job1(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        class Job2(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc1.register_job(Job1)
        qc2.register_job(Job2)

        # Enqueue jobs to different prefixes
        Job1.perform_later(qc1, "data1")
        Job2.perform_later(qc2, "data2")

        # Verify isolation
        from sqlalchemy import create_engine

        sa_url = db_url.split("?")[0] if db_url.startswith("sqlite:") else db_url

        engine = create_engine(sa_url)
        with engine.connect() as conn:
            # app1 should have 1 job
            result = conn.execute(text("SELECT COUNT(*) FROM app1_jobs"))
            assert result.scalar() == 1

            # app2 should have 1 job
            result = conn.execute(text("SELECT COUNT(*) FROM app2_jobs"))
            assert result.scalar() == 1

        engine.dispose()
        qc1.close()
        qc2.close()


class TestJobRegistration:
    """Test job class registration."""

    def test_register_valid_job(self, qc, fake_job_class):
        """Valid ActiveJob subclass should register successfully."""
        qc.register_job(fake_job_class)
        # No exception means success

    def test_register_invalid_job_raises(self, qc):
        """Non-ActiveJob class should raise TypeError."""

        class NotAJob:
            pass

        with pytest.raises(TypeError, match="subclass of"):
            qc.register_job(NotAJob)

    def test_register_multiple_jobs(self, qc):
        """Multiple job classes can be registered."""

        class JobA(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        class JobB(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(JobA)
        qc.register_job(JobB)
        # Both should be registered without error


class TestDBAssertHelper:
    """Test the DBAssert helper class."""

    def test_count_jobs(self, qc_with_sqlalchemy, db_assert, fake_job_class):
        """DBAssert.count_jobs should return correct count."""
        qc = qc_with_sqlalchemy["qc"]
        qc.register_job(fake_job_class)

        assert db_assert.count_jobs() == 0

        fake_job_class.perform_later(qc, "arg1")
        assert db_assert.count_jobs() == 1

        fake_job_class.perform_later(qc, "arg2")
        assert db_assert.count_jobs() == 2

    def test_count_ready_executions(
        self, qc_with_sqlalchemy, db_assert, fake_job_class
    ):
        """DBAssert.count_ready_executions should return correct count."""
        qc = qc_with_sqlalchemy["qc"]
        qc.register_job(fake_job_class)

        assert db_assert.count_ready_executions() == 0

        fake_job_class.perform_later(qc, "arg1")
        assert db_assert.count_ready_executions() == 1

    def test_get_job(self, qc_with_sqlalchemy, db_assert, fake_job_class):
        """DBAssert.get_job should return job data."""
        qc = qc_with_sqlalchemy["qc"]
        qc.register_job(fake_job_class)

        job = fake_job_class.perform_later(qc, "arg1")
        job_data = db_assert.get_job(job.id)

        assert job_data is not None
        assert "FakeJob" in job_data["class_name"]
        assert job_data["id"] == job.id
