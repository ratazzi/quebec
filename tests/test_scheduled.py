# coding: utf-8
"""
Integration tests for scheduled jobs and queue priority.
"""

from datetime import datetime, timedelta, timezone
import quebec
from quebec import BaseClass
from sqlalchemy import text


class TestScheduledJobs:
    """Test scheduled/delayed job functionality."""

    def test_immediate_job_goes_to_ready_executions(self, qc_with_sqlalchemy):
        """Job without delay should go directly to ready_executions."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class ImmediateJob(BaseClass):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(ImmediateJob)
        job = ImmediateJob.perform_later(qc, "immediate")

        # Should be in ready_executions
        result = session.execute(
            text(
                f"SELECT COUNT(*) FROM {prefix}_ready_executions WHERE job_id = :job_id"
            ),
            {"job_id": job.id},
        )
        assert result.scalar() == 1

        # Should NOT be in scheduled_executions
        result = session.execute(
            text(
                f"SELECT COUNT(*) FROM {prefix}_scheduled_executions WHERE job_id = :job_id"
            ),
            {"job_id": job.id},
        )
        assert result.scalar() == 0

    def test_delayed_job_with_wait_goes_to_scheduled(self, qc_with_sqlalchemy):
        """Job with wait should go to scheduled_executions."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class DelayedJob(BaseClass):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(DelayedJob)

        # Enqueue with 1 hour delay
        job = DelayedJob.set(wait=3600).perform_later(qc, "delayed")

        # Should NOT be in ready_executions
        result = session.execute(
            text(
                f"SELECT COUNT(*) FROM {prefix}_ready_executions WHERE job_id = :job_id"
            ),
            {"job_id": job.id},
        )
        assert result.scalar() == 0

        # Should be in scheduled_executions
        result = session.execute(
            text(
                f"SELECT scheduled_at FROM {prefix}_scheduled_executions WHERE job_id = :job_id"
            ),
            {"job_id": job.id},
        )
        scheduled_at_str = result.scalar()
        assert scheduled_at_str is not None

        # Verify scheduled time is approximately 1 hour in the future
        scheduled_at = datetime.fromisoformat(scheduled_at_str)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        diff = (scheduled_at - now).total_seconds()
        assert 3550 < diff < 3650  # Allow 50 seconds tolerance

    def test_delayed_job_with_wait_until(self, qc_with_sqlalchemy):
        """Job with wait_until should go to scheduled_executions."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class FutureJob(BaseClass):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(FutureJob)

        # Schedule for 2 hours in the future
        future_time = datetime.now(timezone.utc) + timedelta(hours=2)
        job = FutureJob.set(wait_until=future_time).perform_later(qc, "future")

        # Should be in scheduled_executions
        result = session.execute(
            text(
                f"SELECT scheduled_at FROM {prefix}_scheduled_executions WHERE job_id = :job_id"
            ),
            {"job_id": job.id},
        )
        scheduled_at_str = result.scalar()
        assert scheduled_at_str is not None

        scheduled_at = datetime.fromisoformat(scheduled_at_str)
        expected = future_time.replace(tzinfo=None)
        diff = abs((scheduled_at - expected).total_seconds())
        assert diff < 2  # Within 2 seconds

    def test_delayed_job_with_timedelta(self, qc_with_sqlalchemy):
        """Job with wait as timedelta should work."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class TimedeltaJob(BaseClass):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(TimedeltaJob)

        job = TimedeltaJob.set(wait=timedelta(minutes=30)).perform_later(qc, "td")

        # Should be in scheduled_executions
        result = session.execute(
            text(
                f"SELECT COUNT(*) FROM {prefix}_scheduled_executions WHERE job_id = :job_id"
            ),
            {"job_id": job.id},
        )
        assert result.scalar() == 1


class TestQueuePriority:
    """Test queue and priority ordering."""

    def test_jobs_ordered_by_priority(self, qc_with_sqlalchemy):
        """Jobs should be ordered by priority (lower number = higher priority)."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class LowPriorityJob(quebec.ActiveJob):
            queue_with_priority = 100

            def perform(self, *args, **kwargs):
                pass

        class HighPriorityJob(quebec.ActiveJob):
            queue_with_priority = 1

            def perform(self, *args, **kwargs):
                pass

        class NormalPriorityJob(quebec.ActiveJob):
            # Default priority is 0
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(LowPriorityJob)
        qc.register_job(HighPriorityJob)
        qc.register_job(NormalPriorityJob)

        # Enqueue in random order
        job_low = LowPriorityJob.perform_later(qc, "low")
        job_high = HighPriorityJob.perform_later(qc, "high")
        job_normal = NormalPriorityJob.perform_later(qc, "normal")

        # Query ready executions ordered by priority
        result = session.execute(
            text(f"""
                SELECT job_id, priority
                FROM {prefix}_ready_executions
                ORDER BY priority ASC, id ASC
            """)
        )
        rows = result.fetchall()

        # Should be ordered: normal (0), high (1), low (100)
        assert len(rows) == 3
        job_ids = [row[0] for row in rows]
        assert job_ids[0] == job_normal.id  # priority 0
        assert job_ids[1] == job_high.id  # priority 1
        assert job_ids[2] == job_low.id  # priority 100

    def test_multiple_queues_isolation(self, qc_with_sqlalchemy):
        """Jobs in different queues should be isolated."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class QueueAJob(quebec.ActiveJob):
            queue_as = "queue_a"

            def perform(self, *args, **kwargs):
                pass

        class QueueBJob(quebec.ActiveJob):
            queue_as = "queue_b"

            def perform(self, *args, **kwargs):
                pass

        qc.register_job(QueueAJob)
        qc.register_job(QueueBJob)

        # Enqueue jobs to different queues
        QueueAJob.perform_later(qc, "a1")
        QueueAJob.perform_later(qc, "a2")
        QueueBJob.perform_later(qc, "b1")

        # Check queue_a has 2 jobs
        result = session.execute(
            text(
                f"SELECT COUNT(*) FROM {prefix}_ready_executions WHERE queue_name = 'queue_a'"
            )
        )
        assert result.scalar() == 2

        # Check queue_b has 1 job
        result = session.execute(
            text(
                f"SELECT COUNT(*) FROM {prefix}_ready_executions WHERE queue_name = 'queue_b'"
            )
        )
        assert result.scalar() == 1
