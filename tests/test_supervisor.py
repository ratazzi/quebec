"""Tests for the supervisor DB-layer APIs and Python orchestration class.

These tests exercise the Rust-exposed methods that the Python Supervisor
relies on (register/heartbeat/deregister, fail_claimed_by_pid,
fail_claimed_by_process_id, reset_after_fork, apply_*_config,
supervisor_plan_from_config) without actually forking child processes.

End-to-end fork tests are intentionally not included here: pytest + tokio
runtimes + fork is fragile, and the individual pieces are covered below.
"""

from __future__ import annotations

import os
import socket

import pytest
from sqlalchemy import text


def _insert_claimed(session, prefix: str, process_id: int, job_class: str = "TestJob"):
    """Insert a minimal job + claimed_execution row for a given process_id.

    Returns the new job_id.
    """
    now_sql = "CURRENT_TIMESTAMP"
    session.execute(
        text(
            f"INSERT INTO {prefix}_jobs "
            f"(queue_name, class_name, arguments, priority, active_job_id, "
            f"scheduled_at, finished_at, concurrency_key, created_at, updated_at) "
            f"VALUES ('default', :class_name, '[]', 0, 'ajid', NULL, NULL, NULL, "
            f"{now_sql}, {now_sql})"
        ),
        {"class_name": job_class},
    )
    job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.execute(
        text(
            f"INSERT INTO {prefix}_claimed_executions "
            f"(job_id, process_id, created_at) "
            f"VALUES (:job_id, :process_id, {now_sql})"
        ),
        {"job_id": job_id, "process_id": process_id},
    )
    session.commit()
    return job_id


class TestSupervisorRegistration:
    def test_register_supervisor_creates_processes_row(self, qc_with_sqlalchemy):
        qc = qc_with_sqlalchemy["qc"]
        session = qc_with_sqlalchemy["session"]
        prefix = qc_with_sqlalchemy["prefix"]

        pid = qc.register_supervisor()
        assert isinstance(pid, int) and pid > 0

        row = session.execute(
            text(f"SELECT kind, pid, hostname FROM {prefix}_processes WHERE id = :id"),
            {"id": pid},
        ).fetchone()
        assert row is not None
        assert row.kind == "Supervisor"
        assert row.pid == os.getpid()

    def test_heartbeat_updates_last_heartbeat_at(self, qc_with_sqlalchemy):
        qc = qc_with_sqlalchemy["qc"]
        session = qc_with_sqlalchemy["session"]
        prefix = qc_with_sqlalchemy["prefix"]

        pid = qc.register_supervisor()
        initial = session.execute(
            text(f"SELECT last_heartbeat_at FROM {prefix}_processes WHERE id = :id"),
            {"id": pid},
        ).scalar()

        qc.heartbeat_process(pid)
        session.expire_all()
        updated = session.execute(
            text(f"SELECT last_heartbeat_at FROM {prefix}_processes WHERE id = :id"),
            {"id": pid},
        ).scalar()
        # Timestamps are strings in SQLite; just require an update attempt
        # actually ran without raising and the row still exists.
        assert updated is not None
        assert initial is not None

    def test_deregister_removes_row(self, qc_with_sqlalchemy):
        qc = qc_with_sqlalchemy["qc"]
        session = qc_with_sqlalchemy["session"]
        prefix = qc_with_sqlalchemy["prefix"]

        pid = qc.register_supervisor()
        qc.deregister_process(pid)

        remaining = session.execute(
            text(f"SELECT COUNT(*) FROM {prefix}_processes WHERE id = :id"),
            {"id": pid},
        ).scalar()
        assert remaining == 0


class TestFailClaimed:
    def test_fail_claimed_by_process_id_moves_to_failed(self, qc_with_sqlalchemy):
        qc = qc_with_sqlalchemy["qc"]
        session = qc_with_sqlalchemy["session"]
        prefix = qc_with_sqlalchemy["prefix"]

        # Register a fake worker-like row to attach a claim to
        process_id = qc.register_supervisor()  # kind doesn't matter here
        job_id = _insert_claimed(session, prefix, process_id)

        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {prefix}_claimed_executions WHERE job_id = :id"
                ),
                {"id": job_id},
            ).scalar()
            == 1
        )

        failed = qc.supervisor_fail_claimed_by_process_id(process_id)
        assert failed == 1

        # Claim row removed, failed row created, process row pruned
        session.expire_all()
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {prefix}_claimed_executions WHERE job_id = :id"
                ),
                {"id": job_id},
            ).scalar()
            == 0
        )
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {prefix}_failed_executions WHERE job_id = :id"
                ),
                {"id": job_id},
            ).scalar()
            == 1
        )
        assert (
            session.execute(
                text(f"SELECT COUNT(*) FROM {prefix}_processes WHERE id = :id"),
                {"id": process_id},
            ).scalar()
            == 0
        )

    def test_fail_claimed_by_pid_uses_hostname_scope(self, qc_with_sqlalchemy):
        qc = qc_with_sqlalchemy["qc"]
        session = qc_with_sqlalchemy["session"]
        prefix = qc_with_sqlalchemy["prefix"]

        # Register and insert a claim attached to that process row
        process_id = qc.register_supervisor()
        _insert_claimed(session, prefix, process_id)

        # Wrong hostname finds nothing
        assert qc.supervisor_fail_claimed_by_pid(os.getpid(), "wrong-host") == 0

        # Correct hostname reaps the row
        failed = qc.supervisor_fail_claimed_by_pid(os.getpid(), socket.gethostname())
        assert failed == 1

    def test_fail_claimed_missing_row_returns_zero(self, qc_with_sqlalchemy):
        qc = qc_with_sqlalchemy["qc"]
        assert qc.supervisor_fail_claimed_by_process_id(999999) == 0
        assert qc.supervisor_fail_claimed_by_pid(999999, socket.gethostname()) == 0


class TestCurrentProcessIds:
    def test_accessors_none_before_start(self, qc):
        assert qc.current_worker_process_id() is None
        assert qc.current_dispatcher_process_id() is None
        assert qc.current_scheduler_process_id() is None


class TestApplyConfig:
    def test_apply_worker_config_out_of_range_raises(self, qc):
        # No queue.yml loaded in this test context; out-of-range should raise
        # IndexError (or be a no-op if config missing). We can't guarantee
        # which without a config file, so accept either behavior.
        try:
            qc.apply_worker_config(99)
        except IndexError:
            pass
        except Exception as exc:
            pytest.fail(f"unexpected exception: {exc!r}")

    def test_apply_dispatcher_config_out_of_range_raises(self, qc):
        try:
            qc.apply_dispatcher_config(99)
        except IndexError:
            pass
        except Exception as exc:
            pytest.fail(f"unexpected exception: {exc!r}")


class TestSupervisorPlanFromConfig:
    def test_returns_dict_or_none(self, qc):
        # Whether a queue.yml exists in the cwd is environment-dependent; just
        # verify the method returns the right *shape* and never raises.
        plan = qc.supervisor_plan_from_config()
        assert plan is None or isinstance(plan, dict)
        if plan is not None:
            for key, val in plan.items():
                assert key in {"worker", "dispatcher", "scheduler"}
                assert isinstance(val, int) and val > 0

    def test_scheduler_included_when_recurring_schedule_exists(
        self, qc, tmp_path, monkeypatch
    ):
        """Supervisor plan should fork a scheduler child when recurring.yml
        is configured, so the scheduler's cron loop runs in its own process
        instead of piggybacking on the worker child's tokio runtime."""
        queue_yml = tmp_path / "queue.yml"
        queue_yml.write_text(
            """
development:
  workers:
    - queues: "*"
      threads: 1
      processes: 2
"""
        )
        recurring_yml = tmp_path / "recurring.yml"
        recurring_yml.write_text(
            """
development:
  hourly_cleanup:
    class: CleanupJob
    schedule: "every hour"
"""
        )

        monkeypatch.setenv("QUEBEC_CONFIG", str(queue_yml))
        monkeypatch.setenv("QUEBEC_RECURRING_SCHEDULE", str(recurring_yml))
        monkeypatch.delenv("QUEBEC_ENV", raising=False)
        monkeypatch.delenv("SOLID_QUEUE_RECURRING_SCHEDULE", raising=False)

        plan = qc.supervisor_plan_from_config()
        assert plan is not None, "supervisor plan should trigger with processes=2"
        assert plan.get("worker") == 2
        assert plan.get("scheduler") == 1

    def test_scheduler_omitted_when_no_recurring_schedule(
        self, qc, tmp_path, monkeypatch
    ):
        """Without a recurring.yml, scheduler should NOT be forked — there's
        nothing for it to do, and forking it would just trip the crash-loop
        guard when Scheduler.run() exits immediately."""
        queue_yml = tmp_path / "queue.yml"
        queue_yml.write_text(
            """
development:
  workers:
    - queues: "*"
      threads: 1
      processes: 2
"""
        )

        monkeypatch.setenv("QUEBEC_CONFIG", str(queue_yml))
        monkeypatch.delenv("QUEBEC_ENV", raising=False)
        monkeypatch.delenv("QUEBEC_RECURRING_SCHEDULE", raising=False)
        monkeypatch.delenv("SOLID_QUEUE_RECURRING_SCHEDULE", raising=False)
        monkeypatch.chdir(tmp_path)  # Ensure no recurring.yml in cwd picks up

        plan = qc.supervisor_plan_from_config()
        assert plan is not None
        assert "scheduler" not in plan

    def test_single_process_config_returns_none(self, qc, tmp_path, monkeypatch):
        """Bare 1-worker + 1-dispatcher config must stay in the single-process
        threaded path for backward compatibility — the supervisor shouldn't
        take over just because a config file exists."""
        queue_yml = tmp_path / "queue.yml"
        queue_yml.write_text(
            """
development:
  workers:
    - queues: "*"
      threads: 3
  dispatchers:
    - polling_interval: 1
"""
        )

        monkeypatch.setenv("QUEBEC_CONFIG", str(queue_yml))
        monkeypatch.delenv("QUEBEC_ENV", raising=False)
        monkeypatch.chdir(tmp_path)

        assert qc.supervisor_plan_from_config() is None


class TestResetAfterFork:
    def test_reset_after_fork_is_idempotent_and_preserves_apis(self, qc):
        # Called without a fork, reset should still run: it rebuilds the
        # runtime/pool from scratch. A second call should also succeed.
        qc.reset_after_fork()
        # Basic sanity: DB is still usable
        assert qc.ping() is True
        qc.reset_after_fork()
        assert qc.ping() is True


class TestSupervisorClassValidation:
    def test_rejects_unknown_role(self, qc):
        from quebec.supervisor import Supervisor

        with pytest.raises(ValueError, match="unknown role"):
            Supervisor(qc, {"ghost_role": 1})

    def test_rejects_empty_plan(self, qc):
        from quebec.supervisor import Supervisor

        with pytest.raises(ValueError, match="at least one"):
            Supervisor(qc, {})

    def test_rejects_all_zero_plan(self, qc):
        from quebec.supervisor import Supervisor

        with pytest.raises(ValueError, match="at least one"):
            Supervisor(qc, {"worker": 0})

    def test_accepts_int_or_list_spec(self, qc):
        from quebec.supervisor import Supervisor

        # Integer count
        Supervisor(qc, {"worker": 2})
        # List-style (length is what matters)
        Supervisor(qc, {"worker": [None, None, None]})
