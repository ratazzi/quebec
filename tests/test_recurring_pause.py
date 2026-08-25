"""Tests for pausing recurring tasks (`recurring_pause=True`).

Solid Queue has no pause state for recurring tasks, so this feature adds a
nullable `paused_at` column to the recurring tasks table. It is opt-in: with
the switch off the schema is untouched and the API raises. With it on, the
column is added by `create_tables()` (and by a starting scheduler), and the
scheduler skips every occurrence of a paused task.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone

import pytest

import quebec

from .helpers import wait_until


class TickJob(quebec.BaseClass):
    def perform(self, *args, **kwargs) -> None:
        return None


def _db_path(db_url: str) -> str:
    assert db_url.startswith("sqlite:///")
    return db_url.removeprefix("sqlite:///").split("?", 1)[0]


def _columns(db_url: str, table: str) -> list[str]:
    with sqlite3.connect(_db_path(db_url)) as conn:
        return [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]


def _count(db_url: str, table: str) -> int:
    with sqlite3.connect(_db_path(db_url)) as conn:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def _paused_at(db_url: str, prefix: str, key: str):
    with sqlite3.connect(_db_path(db_url)) as conn:
        return conn.execute(
            f'SELECT paused_at FROM "{prefix}_recurring_tasks" WHERE "key" = ?',
            (key,),
        ).fetchone()[0]


def _seed_task(db_url: str, prefix: str, key: str, class_name: str) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ")
    with sqlite3.connect(_db_path(db_url)) as conn:
        conn.execute(
            f'INSERT INTO "{prefix}_recurring_tasks" '
            '("key", schedule, class_name, arguments, queue_name, priority, '
            '"static", created_at, updated_at) '
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (key, "every minute", class_name, "[]", "default", 0, 1, now, now),
        )


def test_off_by_default_leaves_schema_alone(db_url, test_prefix) -> None:
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert qc.create_tables() is True
        assert "paused_at" not in _columns(db_url, f"{test_prefix}_recurring_tasks")

        with pytest.raises(RuntimeError, match="recurring_pause=True"):
            qc.pause_recurring("anything")
        with pytest.raises(RuntimeError):
            qc.paused_recurring_tasks()
    finally:
        qc.close()


def test_create_tables_adds_the_column_idempotently(db_url, test_prefix) -> None:
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
    try:
        assert qc.create_tables() is True
        assert "paused_at" in _columns(db_url, f"{test_prefix}_recurring_tasks")
        # Re-running against the migrated schema must not fail on the ALTER.
        assert qc.create_tables() is True
        assert qc.paused_recurring_tasks() == []
    finally:
        qc.close()


def test_env_var_opts_in(db_url, test_prefix, monkeypatch) -> None:
    monkeypatch.setenv("QUEBEC_RECURRING_PAUSE", "true")
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert qc.create_tables() is True
        assert "paused_at" in _columns(db_url, f"{test_prefix}_recurring_tasks")
    finally:
        qc.close()


def test_existing_column_is_picked_up_without_create_tables(
    db_url, test_prefix
) -> None:
    """A second process on an already-migrated database must not need
    `create_tables()`: the first API call detects the column."""
    first = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
    try:
        assert first.create_tables() is True
        _seed_task(db_url, test_prefix, "report", TickJob.__qualname__)
    finally:
        first.close()

    second = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
    try:
        assert second.pause_recurring("report") is True
        assert second.recurring_paused("report") is True
    finally:
        second.close()


def test_create_tables_recovers_from_an_early_probe_failure(
    db_url, test_prefix
) -> None:
    """An API call before the tables exist fails the first probe (nothing to
    ALTER yet). That failure must not be sticky: `create_tables()` re-applies
    the column and the same instance can pause afterwards."""
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
    try:
        with pytest.raises(RuntimeError, match="could not be added"):
            qc.paused_recurring_tasks()
        assert qc.create_tables() is True
        assert qc.paused_recurring_tasks() == []
    finally:
        qc.close()


def test_api_retries_once_tables_exist_elsewhere(db_url, test_prefix) -> None:
    """An explicit call is a fresh attempt: after the first probe failed for
    lack of tables, a later call finds the tables another process created and
    adds the column itself, without `create_tables()` on this instance."""
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
    try:
        with pytest.raises(RuntimeError, match="could not be added"):
            qc.paused_recurring_tasks()

        other = quebec.Quebec(db_url, table_name_prefix=test_prefix)
        try:
            assert other.create_tables() is True  # switch off: no column
        finally:
            other.close()
        assert "paused_at" not in _columns(db_url, f"{test_prefix}_recurring_tasks")

        assert qc.paused_recurring_tasks() == []
        assert "paused_at" in _columns(db_url, f"{test_prefix}_recurring_tasks")
    finally:
        qc.close()


def test_pause_and_resume_round_trip(db_url, test_prefix) -> None:
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
    try:
        assert qc.create_tables() is True
        _seed_task(db_url, test_prefix, "report", TickJob.__qualname__)
        _seed_task(db_url, test_prefix, "cleanup", TickJob.__qualname__)

        assert qc.recurring_paused("report") is False
        assert qc.pause_recurring("report") is True
        assert qc.pause_recurring("report") is False  # already paused
        assert qc.recurring_paused("report") is True
        assert qc.paused_recurring_tasks() == ["report"]
        # Re-applying the schema must not reset the pause.
        assert qc.create_tables() is True
        assert qc.recurring_paused("report") is True

        assert qc.resume_recurring("report") is True
        assert qc.resume_recurring("report") is False  # already running
        assert qc.recurring_paused("report") is False
        assert qc.paused_recurring_tasks() == []

        for call in (qc.pause_recurring, qc.resume_recurring, qc.recurring_paused):
            with pytest.raises(LookupError, match="missing"):
                call("missing")
    finally:
        qc.close()


def test_run_now_ignores_the_pause(db_url, test_prefix) -> None:
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
    try:
        assert qc.create_tables() is True
        qc.register_job(TickJob)
        _seed_task(db_url, test_prefix, "report", TickJob.__qualname__)

        assert qc.pause_recurring("report") is True
        # An explicit manual run is not an occurrence of the schedule.
        assert qc.run_recurring_now("report") is True
        assert _count(db_url, f"{test_prefix}_jobs") == 1
    finally:
        qc.close()


def test_scheduler_skips_occurrences_while_paused(temp_db_path, test_prefix) -> None:
    db_url = f"sqlite:///{temp_db_path}?mode=rwc"
    jobs_table = f"{test_prefix}_jobs"
    executions_table = f"{test_prefix}_recurring_executions"

    # Every 2 seconds: quick enough to observe skipping and resuming, coarse
    # enough to tell "the next slot" from "the slot after that".
    recurring_config = f"""
test:
  tick:
    class: {TickJob.__qualname__}
    schedule: every 2 seconds
    queue: default
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(recurring_config)
        recurring_path = f.name

    qc = None
    try:
        os.environ["QUEBEC_RECURRING_SCHEDULE"] = recurring_path
        os.environ["QUEBEC_ENV"] = "test"

        qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
        assert qc.create_tables() is True
        qc.register_job(TickJob)
        qc.spawn_scheduler()

        wait_until(
            lambda: _count(db_url, jobs_table) >= 1,
            timeout=5,
            message="scheduler never enqueued the tick task",
        )

        assert qc.pause_recurring("tick") is True
        # A tick may already have been past the pause check; let it land.
        time.sleep(1.2)
        frozen = _count(db_url, jobs_table)
        frozen_runs = _count(db_url, executions_table)
        time.sleep(2.5)
        assert _count(db_url, jobs_table) == frozen, "paused task was still enqueued"
        assert _count(db_url, executions_table) == frozen_runs, (
            "paused occurrence was recorded as a recurring execution"
        )

        resumed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        assert qc.resume_recurring("tick") is True
        wait_until(
            lambda: _count(db_url, jobs_table) > frozen,
            timeout=5,
            message="scheduler did not resume enqueueing",
        )

        # Plain cron semantics, as with un-commenting a crontab line: the
        # first run after resuming is the very next occurrence after the
        # resume — not a replay of a skipped one, and not the one after next.
        with sqlite3.connect(_db_path(db_url)) as conn:
            (first_run_at,) = conn.execute(
                f'SELECT MIN(run_at) FROM "{executions_table}" WHERE run_at > ?',
                (resumed_at.isoformat(sep=" "),),
            ).fetchone()
        assert first_run_at is not None
        delay = datetime.fromisoformat(first_run_at) - resumed_at
        assert timedelta(0) < delay <= timedelta(seconds=2), delay
    finally:
        if qc is not None:
            qc.close()
        os.unlink(recurring_path)
        os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
        os.environ.pop("QUEBEC_ENV", None)


def test_scheduler_restart_keeps_the_pause(temp_db_path, test_prefix) -> None:
    """A starting scheduler upserts the static tasks from YAML and deletes
    the ones no longer configured. Neither may touch `paused_at`: a paused
    task must still be paused after a restart, not silently running again.
    """
    db_url = f"sqlite:///{temp_db_path}?mode=rwc"
    jobs_table = f"{test_prefix}_jobs"
    tasks_table = f"{test_prefix}_recurring_tasks"

    recurring_config = f"""
test:
  tick:
    class: {TickJob.__qualname__}
    schedule: "* * * * * *"
    queue: default
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(recurring_config)
        recurring_path = f.name

    def start() -> quebec.Quebec:
        qc = quebec.Quebec(db_url, table_name_prefix=test_prefix, recurring_pause=True)
        assert qc.create_tables() is True
        qc.register_job(TickJob)
        qc.spawn_scheduler()
        wait_until(
            lambda: _count(db_url, tasks_table) == 1,
            timeout=5,
            message="scheduler did not sync the recurring task",
        )
        return qc

    first = second = None
    try:
        os.environ["QUEBEC_RECURRING_SCHEDULE"] = recurring_path
        os.environ["QUEBEC_ENV"] = "test"

        first = start()
        assert first.pause_recurring("tick") is True
        paused_at = _paused_at(db_url, test_prefix, "tick")
        assert paused_at is not None
        first.close()
        first = None

        time.sleep(1.2)  # let any in-flight tick from the old process land
        frozen = _count(db_url, jobs_table)

        second = start()
        assert second.recurring_paused("tick") is True
        assert _paused_at(db_url, test_prefix, "tick") == paused_at

        time.sleep(2.5)
        assert _count(db_url, jobs_table) == frozen, (
            "task resumed silently after a scheduler restart"
        )
        assert second.paused_recurring_tasks() == ["tick"]
    finally:
        for qc in (first, second):
            if qc is not None:
                qc.close()
        os.unlink(recurring_path)
        os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
        os.environ.pop("QUEBEC_ENV", None)


def test_scheduler_honours_a_pause_after_its_own_probe_failed(
    temp_db_path, test_prefix
) -> None:
    """A scheduler whose startup probe failed must not ignore pauses forever:
    once another process adds the column and pauses the task, this scheduler
    has to notice within the re-probe interval and stop enqueueing."""
    db_url = f"sqlite:///{temp_db_path}?mode=rwc"
    jobs_table = f"{test_prefix}_jobs"
    reprobe_secs = 5  # RECURRING_PAUSE_REPROBE_SECS in src/context.rs

    recurring_config = f"""
test:
  tick:
    class: {TickJob.__qualname__}
    schedule: every 2 seconds
    queue: default
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(recurring_config)
        recurring_path = f.name

    stale = migrated = None
    try:
        os.environ["QUEBEC_RECURRING_SCHEDULE"] = recurring_path
        os.environ["QUEBEC_ENV"] = "test"

        # Probe before any table exists: the ALTER fails and is never retried.
        stale = quebec.Quebec(
            db_url, table_name_prefix=test_prefix, recurring_pause=True
        )
        with pytest.raises(RuntimeError, match="could not be added"):
            stale.paused_recurring_tasks()

        plain = quebec.Quebec(db_url, table_name_prefix=test_prefix)
        try:
            assert plain.create_tables() is True  # tables, but no column
        finally:
            plain.close()

        stale.register_job(TickJob)
        stale.spawn_scheduler()
        wait_until(
            lambda: _count(db_url, jobs_table) >= 1,
            timeout=5,
            message="scheduler never enqueued the tick task",
        )

        migrated = quebec.Quebec(
            db_url, table_name_prefix=test_prefix, recurring_pause=True
        )
        assert migrated.create_tables() is True  # adds the column
        assert migrated.pause_recurring("tick") is True

        # Worst case: a re-probe just happened, so the next one is a full
        # interval away, then one more tick to observe the pause.
        time.sleep(reprobe_secs + 2.5)
        frozen = _count(db_url, jobs_table)
        time.sleep(2.5)
        assert _count(db_url, jobs_table) == frozen, (
            "scheduler kept enqueueing after the column and the pause appeared"
        )
    finally:
        for qc in (stale, migrated):
            if qc is not None:
                qc.close()
        os.unlink(recurring_path)
        os.environ.pop("QUEBEC_RECURRING_SCHEDULE", None)
        os.environ.pop("QUEBEC_ENV", None)
