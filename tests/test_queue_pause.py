"""Tests for the queue pause/resume Python API on the Quebec instance."""

from __future__ import annotations

import pytest
from sqlalchemy import text

import quebec


def test_pause_queue_inserts_row_and_resume_deletes_it(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    assert qc.queue_paused("emails") is False
    assert qc.paused_queues() == []

    assert qc.pause_queue("emails") is True

    assert qc.queue_paused("emails") is True
    paused = session.execute(
        text(f"SELECT queue_name FROM {prefix}_pauses ORDER BY queue_name")
    ).fetchall()
    assert [row.queue_name for row in paused] == ["emails"]
    assert qc.paused_queues() == ["emails"]

    assert qc.resume_queue("emails") is True

    assert qc.queue_paused("emails") is False
    assert qc.paused_queues() == []


def test_pause_queue_is_idempotent(qc) -> None:
    assert qc.pause_queue("emails") is True
    assert qc.pause_queue("emails") is False
    assert qc.queue_paused("emails") is True


def test_resume_queue_returns_false_when_not_paused(qc) -> None:
    assert qc.resume_queue("emails") is False
    assert qc.queue_paused("emails") is False


def test_pause_all_skips_already_paused_and_resume_all_clears(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    # pause_all enumerates queues from the jobs table — seed two queues
    # directly via SQLAlchemy to keep the test free of Quebec's enqueue path.
    for queue in ("emails", "reports"):
        session.execute(
            text(
                f"INSERT INTO {prefix}_jobs "
                f"(queue_name, class_name, arguments, priority, created_at, updated_at) "
                f"VALUES (:queue, 'Job', '[]', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            ),
            {"queue": queue},
        )
    session.commit()

    # Pre-pause one queue manually to confirm pause_all skips it.
    assert qc.pause_queue("emails") is True

    newly_paused = qc.pause_all()
    assert newly_paused == 1
    assert sorted(qc.paused_queues()) == ["emails", "reports"]

    resumed = qc.resume_all()
    assert resumed == 2
    assert qc.paused_queues() == []


class PauseJob(quebec.BaseClass):
    def perform(self, *args, **kwargs) -> None:
        pass


def test_paused_queue_is_skipped_by_single_claim(qc) -> None:
    # A `*` worker expands into per-queue polls once anything is paused
    # (Solid Queue's QueueSelector semantics), so the paused queue is never
    # a claim candidate — not even when its job sorts first by job_id.
    qc.register_job(PauseJob)
    PauseJob.set(queue="paused_q").perform_later(qc)
    PauseJob.set(queue="live_a").perform_later(qc)
    PauseJob.set(queue="live_b").perform_later(qc)

    assert qc.pause_queue("paused_q") is True

    claimed = []
    for _ in range(2):
        execution = qc.drain_one()
        claimed.append(execution.queue)
        execution.perform()

    assert sorted(claimed) == ["live_a", "live_b"]

    with pytest.raises(RuntimeError):
        qc.drain_one()

    assert qc.resume_queue("paused_q") is True
    execution = qc.drain_one()
    assert execution.queue == "paused_q"
    execution.perform()


def test_paused_queue_is_skipped_by_batch_claim(qc) -> None:
    qc.register_job(PauseJob)
    PauseJob.set(queue="paused_q").perform_later(qc)
    PauseJob.set(queue="live_a").perform_later(qc)
    PauseJob.set(queue="live_b").perform_later(qc)

    assert qc.pause_queue("paused_q") is True

    claimed = qc.drain_batch(10)
    assert sorted(e.queue for e in claimed) == ["live_a", "live_b"]
    for execution in claimed:
        execution.perform()

    assert qc.drain_batch(10) == []
