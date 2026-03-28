# coding: utf-8
"""
Integration tests for failed jobs and retry functionality.

Tests that failed jobs are properly recorded and can be retried/discarded.
"""

import json
from datetime import timedelta

import quebec
from sqlalchemy import text

from .helpers import get_job_by_active_job_id


class TestFailedJobs:
    """Test failed job handling."""

    def test_failed_job_recorded_in_failed_executions(self, qc_with_sqlalchemy):
        """When a job fails, it should be recorded in failed_executions."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class FailingJob(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                raise ValueError("Intentional failure")

        qc.register_job(FailingJob)
        enqueued = FailingJob.perform_later(qc, "fail_arg")
        execution = qc.drain_one()
        execution.perform()
        session.expire_all()

        failed_execution = session.execute(
            text(
                f"SELECT error FROM {prefix}_failed_executions WHERE job_id = :job_id"
            ),
            {"job_id": enqueued.id},
        ).fetchone()
        persisted_job = get_job_by_active_job_id(
            session, prefix, enqueued.active_job_id
        )
        arguments = json.loads(persisted_job["arguments"])

        assert failed_execution is not None
        error_payload = json.loads(failed_execution.error)
        assert error_payload["exception_class"] == "ValueError"
        assert error_payload["message"] == "Intentional failure"
        assert arguments["executions"] == 1
        assert arguments["exception_executions"]["[ValueError]"] == 1

    def test_job_with_retry_on_exception(self, qc_with_sqlalchemy):
        """Job with retry_on should be configured for retry on specific exceptions."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class RetryableJob(quebec.ActiveJob):
            retry_on = [
                quebec.RetryStrategy(
                    (ValueError,),
                    wait=timedelta(seconds=1),
                    attempts=3,
                    handler=None,
                )
            ]

            def perform(self, *args, **kwargs):
                raise ValueError("Retry me")

        qc.register_job(RetryableJob)
        enqueued = RetryableJob.perform_later(qc, "retry_arg")
        execution = qc.drain_one()
        execution.perform()
        session.expire_all()

        rows = session.execute(
            text(
                f"SELECT arguments, finished_at, scheduled_at "
                f"FROM {prefix}_jobs WHERE active_job_id = :active_job_id ORDER BY id"
            ),
            {"active_job_id": enqueued.active_job_id},
        ).fetchall()
        original_arguments = json.loads(rows[0].arguments)
        retried_arguments = json.loads(rows[1].arguments)

        assert len(rows) == 2
        assert rows[0].finished_at is not None
        assert rows[1].scheduled_at is not None
        assert original_arguments.get("executions", 0) == 0
        assert retried_arguments["executions"] == 1
        assert retried_arguments["exception_executions"]["[ValueError]"] == 1


class TestJobArguments:
    """Test job argument serialization."""

    def test_job_with_simple_args(self, qc_with_sqlalchemy):
        """Job arguments should be properly serialized."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class SimpleArgsJob(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(SimpleArgsJob)
        job = SimpleArgsJob.perform_later(qc, "arg1", 42, True)

        result = session.execute(
            text(f"SELECT arguments FROM {prefix}_jobs WHERE id = :id"), {"id": job.id}
        )
        args_json = result.scalar()
        assert args_json is not None

        payload = json.loads(args_json)
        assert payload["arguments"]["arguments"] == ["arg1", 42, True]

    def test_job_with_kwargs(self, qc_with_sqlalchemy):
        """Job keyword arguments should be properly serialized."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class KwargsJob(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(KwargsJob)
        job = KwargsJob.perform_later(qc, name="test", value=123)

        result = session.execute(
            text(f"SELECT arguments FROM {prefix}_jobs WHERE id = :id"), {"id": job.id}
        )
        args_json = result.scalar()
        assert args_json is not None

        payload = json.loads(args_json)
        assert payload["arguments"]["arguments"] == [
            {"name": "test", "value": 123, "_quebec_kwargs": True}
        ]

    def test_job_with_complex_args(self, qc_with_sqlalchemy):
        """Job with complex arguments (dict, list) should serialize correctly."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class ComplexArgsJob(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(ComplexArgsJob)
        job = ComplexArgsJob.perform_later(
            qc, {"nested": {"key": "value"}}, [1, 2, 3], option="enabled"
        )

        result = session.execute(
            text(f"SELECT arguments FROM {prefix}_jobs WHERE id = :id"), {"id": job.id}
        )
        args_json = result.scalar()
        assert args_json is not None

        payload = json.loads(args_json)
        assert payload["arguments"]["arguments"] == [
            {"nested": {"key": "value"}},
            [1, 2, 3],
            {"option": "enabled", "_quebec_kwargs": True},
        ]


class TestJobMetadata:
    """Test job metadata fields."""

    def test_job_has_active_job_id(self, qc_with_sqlalchemy):
        """Each job should have a unique active_job_id (UUID)."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class MetadataJob(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(MetadataJob)

        job1 = MetadataJob.perform_later(qc, "job1")
        job2 = MetadataJob.perform_later(qc, "job2")

        result = session.execute(
            text(f"SELECT active_job_id FROM {prefix}_jobs WHERE id IN (:id1, :id2)"),
            {"id1": job1.id, "id2": job2.id},
        )
        job_ids = [row[0] for row in result.fetchall()]

        assert len(job_ids) == 2
        assert job_ids[0] is not None
        assert job_ids[1] is not None
        # Each job should have unique active_job_id
        assert job_ids[0] != job_ids[1]

    def test_job_has_timestamps(self, qc_with_sqlalchemy):
        """Job should have created_at and updated_at timestamps."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class TimestampJob(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(TimestampJob)
        job = TimestampJob.perform_later(qc, "timestamp_arg")

        result = session.execute(
            text(f"SELECT created_at, updated_at FROM {prefix}_jobs WHERE id = :id"),
            {"id": job.id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] is not None  # created_at
        assert row[1] is not None  # updated_at

    def test_job_finished_at_is_null_initially(self, qc_with_sqlalchemy):
        """Job finished_at should be NULL until job completes."""
        ctx = qc_with_sqlalchemy
        qc = ctx["qc"]
        session = ctx["session"]
        prefix = ctx["prefix"]

        class UnfinishedJob(quebec.ActiveJob):
            def perform(self, *args, **kwargs):
                pass

        qc.register_job(UnfinishedJob)
        job = UnfinishedJob.perform_later(qc, "unfinished_arg")

        result = session.execute(
            text(f"SELECT finished_at FROM {prefix}_jobs WHERE id = :id"),
            {"id": job.id},
        )
        assert result.scalar() is None
