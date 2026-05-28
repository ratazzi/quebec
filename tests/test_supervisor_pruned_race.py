"""Regression test: a crashed fork's claimed jobs must still be failed even
when the supervisor's own ``processes`` row has already been pruned.

This mirrors Solid Queue commit 9f12681. In Solid Queue the supervisor looked
up the terminated fork via ``process.supervisees.find_by(name:)``; if the
supervisor's own row had been pruned (e.g. a peer worker's maintenance loop
reaped it after a missed heartbeat from a sleep/wake), ``supervisees`` returned
nothing and the dead fork's claimed executions were never failed. The fix made
the lookup global (``SolidQueue::Process.find_by(name:)``).

Quebec's fail path is already global: ``supervisor_fail_claimed_by_pid`` looks
up the *child's* row by (pid, hostname) and fails claims scoped to that child's
process_id, never consulting the supervisor's row or ``supervisor_id``. This
test pins that invariant so a future refactor cannot reintroduce the scoping.
"""

from __future__ import annotations

from sqlalchemy import text


def _insert_process(session, prefix: str, *, kind: str, pid: int, hostname: str) -> int:
    """Insert a processes row with an explicit pid/hostname; return its id."""
    session.execute(
        text(
            f"INSERT INTO {prefix}_processes "
            f"(kind, last_heartbeat_at, pid, hostname, metadata, created_at, name) "
            f"VALUES (:kind, CURRENT_TIMESTAMP, :pid, :hostname, NULL, "
            f"CURRENT_TIMESTAMP, :name)"
        ),
        {"kind": kind, "pid": pid, "hostname": hostname, "name": f"{kind}-{pid}"},
    )
    row_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.commit()
    return row_id


def _insert_claimed(session, prefix: str, process_id: int) -> int:
    """Insert a minimal job + claimed_execution for a process_id; return job_id."""
    session.execute(
        text(
            f"INSERT INTO {prefix}_jobs "
            f"(queue_name, class_name, arguments, priority, active_job_id, "
            f"scheduled_at, finished_at, concurrency_key, created_at, updated_at) "
            f"VALUES ('default', 'TestJob', '[]', 0, 'ajid', NULL, NULL, NULL, "
            f"CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
        )
    )
    job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
    session.execute(
        text(
            f"INSERT INTO {prefix}_claimed_executions "
            f"(job_id, process_id, created_at) "
            f"VALUES (:job_id, :process_id, CURRENT_TIMESTAMP)"
        ),
        {"job_id": job_id, "process_id": process_id},
    )
    session.commit()
    return job_id


class TestSupervisorPrunedRace:
    def test_fork_claims_fail_even_after_supervisor_row_pruned(
        self, qc_with_sqlalchemy
    ):
        qc = qc_with_sqlalchemy["qc"]
        session = qc_with_sqlalchemy["session"]
        prefix = qc_with_sqlalchemy["prefix"]
        hostname = "test-host"

        # A supervisor row and a forked-worker row on the same host. The fork
        # owns a claimed execution.
        supervisor_id = _insert_process(
            session, prefix, kind="Supervisor", pid=11111, hostname=hostname
        )
        fork_pid = 22222
        fork_id = _insert_process(
            session, prefix, kind="Worker", pid=fork_pid, hostname=hostname
        )
        job_id = _insert_claimed(session, prefix, fork_id)

        # Simulate the race: a peer's maintenance loop already pruned the
        # supervisor's own processes row (missed heartbeat after sleep/wake).
        session.execute(
            text(f"DELETE FROM {prefix}_processes WHERE id = :id"),
            {"id": supervisor_id},
        )
        session.commit()
        assert (
            session.execute(
                text(f"SELECT COUNT(*) FROM {prefix}_processes WHERE id = :id"),
                {"id": supervisor_id},
            ).scalar()
            == 0
        ), "supervisor row should be gone before the fork is reaped"

        # The supervisor notices the fork died and reaps it by (pid, hostname).
        # This is the path the bug would have broken if it were scoped to the
        # supervisor's (now-deleted) row. It must still fail the fork's claim.
        failed = qc.supervisor_fail_claimed_by_pid(fork_pid, hostname)
        assert failed == 1, (
            "dead fork's claimed execution must still be failed even though the "
            "supervisor's own processes row was pruned — the lookup is global by "
            "(pid, hostname), not scoped to supervisor_id"
        )

        session.expire_all()
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {prefix}_claimed_executions WHERE job_id = :id"
                ),
                {"id": job_id},
            ).scalar()
            == 0
        ), "claim row should be removed"
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {prefix}_failed_executions WHERE job_id = :id"
                ),
                {"id": job_id},
            ).scalar()
            == 1
        ), "a failed execution should be recorded so the job can be retried"
        assert (
            session.execute(
                text(f"SELECT COUNT(*) FROM {prefix}_processes WHERE id = :id"),
                {"id": fork_id},
            ).scalar()
            == 0
        ), "the dead fork's process row should be pruned"

    def test_orphaned_claims_swept_when_process_row_already_gone(
        self, qc_with_sqlalchemy
    ):
        """Maintenance safety net: a claim whose process row no longer exists is
        failed regardless of which supervisor (if any) owns the sweep."""
        qc = qc_with_sqlalchemy["qc"]
        session = qc_with_sqlalchemy["session"]
        prefix = qc_with_sqlalchemy["prefix"]

        # A fork row + claim, then delete the fork row directly so the claim is
        # orphaned (no backing process). No supervisor row exists at all.
        fork_id = _insert_process(
            session, prefix, kind="Worker", pid=33333, hostname="test-host"
        )
        job_id = _insert_claimed(session, prefix, fork_id)
        session.execute(
            text(f"DELETE FROM {prefix}_processes WHERE id = :id"),
            {"id": fork_id},
        )
        session.commit()

        # run_maintenance with no excluded id: the orphan sweep is global.
        pruned, orphaned = qc.supervisor_run_maintenance(None)
        assert orphaned == 1, "orphaned claim must be failed by the global sweep"

        session.expire_all()
        assert (
            session.execute(
                text(
                    f"SELECT COUNT(*) FROM {prefix}_failed_executions WHERE job_id = :id"
                ),
                {"id": job_id},
            ).scalar()
            == 1
        )
