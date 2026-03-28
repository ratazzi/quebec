# coding: utf-8
"""
Integration tests for recurring tasks configuration.

Tests that Scheduler correctly loads recurring.yml and writes to database.
"""

import os
import tempfile
import quebec
from sqlalchemy import create_engine, text

from .helpers import wait_until


class TestRecurringConfig:
    """Test recurring tasks configuration loading via Scheduler."""

    def test_scheduler_loads_recurring_yaml(self, temp_db_path, test_prefix):
        """Scheduler should load recurring.yml and insert tasks into database."""
        qc = None
        engine = None

        # Create a temporary recurring.yml file
        recurring_config = """
test:
  cleanup_job:
    class: CleanupJob
    schedule: every minute
    queue: default

  health_check:
    class: HealthCheckJob
    args: [42, "test"]
    schedule: every 30 seconds
    queue: monitoring
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(recurring_config)
            recurring_path = f.name

        try:
            # Set environment variables
            os.environ["QUEBEC_RECURRING_SCHEDULE"] = recurring_path
            os.environ["QUEBEC_ENV"] = "test"

            # Create Quebec instance with unique prefix
            db_url = f"sqlite://{temp_db_path}?mode=rwc"
            qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
            qc.create_tables()

            # Register a dummy job class (required for scheduler to run)
            class DummyJob(quebec.ActiveJob):
                def perform(self, *args, **kwargs):
                    pass

            qc.register_job(DummyJob)

            # Start scheduler (spawns async task)
            qc.spawn_scheduler()

            sa_url = f"sqlite:///{temp_db_path}"
            engine = create_engine(sa_url)

            def fetch_tasks():
                with engine.connect() as conn:
                    result = conn.execute(
                        text(
                            f"SELECT * FROM {test_prefix}_recurring_tasks ORDER BY key"
                        )
                    )
                    return [dict(row._mapping) for row in result.fetchall()]

            wait_until(
                lambda: len(fetch_tasks()) == 2,
                timeout=5,
                message="scheduler did not load recurring tasks",
            )
            tasks = fetch_tasks()

            engine.dispose()

            assert len(tasks) == 2

            # Check cleanup_job
            cleanup = next((t for t in tasks if t["key"] == "cleanup_job"), None)
            assert cleanup is not None
            assert cleanup["class_name"] == "CleanupJob"
            assert cleanup["schedule"] == "every minute"

            # Check health_check
            health = next((t for t in tasks if t["key"] == "health_check"), None)
            assert health is not None
            assert health["class_name"] == "HealthCheckJob"
            assert health["schedule"] == "every 30 seconds"

        finally:
            if engine is not None:
                engine.dispose()
            if qc is not None:
                qc.close()
            os.unlink(recurring_path)
            os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
            os.environ.pop("QUEBEC_ENV", None)

    def test_recurring_task_with_complex_args(self, temp_db_path, test_prefix):
        """Recurring task with complex args should be properly stored."""
        qc = None
        engine = None

        recurring_config = """
test:
  complex_job:
    class: ComplexJob
    args: [123, "hello", {"key": "value"}]
    schedule: every hour
    queue: batch
    priority: 10
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(recurring_config)
            recurring_path = f.name

        try:
            os.environ["QUEBEC_RECURRING_SCHEDULE"] = recurring_path
            os.environ["QUEBEC_ENV"] = "test"

            db_url = f"sqlite://{temp_db_path}?mode=rwc"
            qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
            qc.create_tables()

            class DummyJob(quebec.ActiveJob):
                def perform(self, *args, **kwargs):
                    pass

            qc.register_job(DummyJob)

            # Start scheduler
            qc.spawn_scheduler()

            sa_url = f"sqlite:///{temp_db_path}"
            engine = create_engine(sa_url)

            def fetch_task():
                with engine.connect() as conn:
                    result = conn.execute(
                        text(
                            f"SELECT * FROM {test_prefix}_recurring_tasks WHERE key = 'complex_job'"
                        )
                    )
                    return result.fetchone()

            wait_until(
                lambda: fetch_task() is not None,
                timeout=5,
                message="scheduler did not persist the recurring task",
            )
            task = fetch_task()
            engine.dispose()

            assert task is not None
            task_data = dict(task._mapping)
            assert task_data["class_name"] == "ComplexJob"
            assert task_data["queue_name"] == "batch"
            assert task_data["priority"] == 10

            # Check args JSON contains expected values
            import json

            args = json.loads(task_data["arguments"])
            assert 123 in args
            assert "hello" in args

        finally:
            if engine is not None:
                engine.dispose()
            if qc is not None:
                qc.close()
            os.unlink(recurring_path)
            os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
            os.environ.pop("QUEBEC_ENV", None)

    def test_loads_correct_environment(self, temp_db_path, test_prefix):
        """Should load tasks for the specified environment only."""
        qc = None
        engine = None

        recurring_config = """
development:
  dev_job:
    class: DevJob
    schedule: every minute

production:
  prod_job:
    class: ProdJob
    schedule: every hour

test:
  test_job:
    class: TestJob
    schedule: every 30 seconds
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(recurring_config)
            recurring_path = f.name

        try:
            os.environ["QUEBEC_RECURRING_SCHEDULE"] = recurring_path
            os.environ["QUEBEC_ENV"] = "production"

            db_url = f"sqlite://{temp_db_path}?mode=rwc"
            qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
            qc.create_tables()

            class DummyJob(quebec.ActiveJob):
                def perform(self, *args, **kwargs):
                    pass

            qc.register_job(DummyJob)

            qc.spawn_scheduler()

            sa_url = f"sqlite:///{temp_db_path}"
            engine = create_engine(sa_url)

            def fetch_tasks():
                with engine.connect() as conn:
                    result = conn.execute(
                        text(
                            f"SELECT key, class_name FROM {test_prefix}_recurring_tasks"
                        )
                    )
                    return {row[0]: row[1] for row in result.fetchall()}

            wait_until(
                lambda: "prod_job" in fetch_tasks(),
                timeout=5,
                message="scheduler did not load the production recurring tasks",
            )
            tasks = fetch_tasks()
            engine.dispose()

            assert "prod_job" in tasks
            assert tasks["prod_job"] == "ProdJob"
            # Should not have dev or test jobs
            assert "dev_job" not in tasks
            assert "test_job" not in tasks

        finally:
            if engine is not None:
                engine.dispose()
            if qc is not None:
                qc.close()
            os.unlink(recurring_path)
            os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
            os.environ.pop("QUEBEC_ENV", None)
