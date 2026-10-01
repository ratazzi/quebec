from __future__ import annotations

from datetime import datetime, timedelta, timezone

import quebec
from sqlalchemy import text


class FinishedCleanupJob(quebec.BaseClass):
    def perform(self, value: int) -> None:
        return None


def test_count_clearable_jobs_and_clear_finished_jobs(
    qc_with_sqlalchemy, db_assert
) -> None:
    qc = qc_with_sqlalchemy["qc"]

    qc.register_job(FinishedCleanupJob)

    enqueued = FinishedCleanupJob.perform_later(qc, 10)
    execution = qc.drain_one()
    execution.perform()

    assert db_assert.count_jobs() == 1
    assert qc.count_clearable_jobs(finished_before=4102444800.0) == 1

    deleted = qc.clear_finished_jobs(finished_before=4102444800.0)

    assert deleted == 1
    assert db_assert.count_jobs() == 0
    assert qc.count_clearable_jobs(finished_before=4102444800.0) == 0
    assert enqueued.id is not None


def test_clear_finished_jobs_crosses_batch_boundary_and_keeps_newer_jobs(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]
    qc.register_job(FinishedCleanupJob)
    qc.perform_all_later([FinishedCleanupJob.build(index) for index in range(503)])

    ids = (
        session.execute(text(f"SELECT id FROM {prefix}_jobs ORDER BY id"))
        .scalars()
        .all()
    )
    assert len(ids) == 503
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=14)
    session.execute(
        text(f"UPDATE {prefix}_jobs SET finished_at = :finished WHERE id <= :last_old"),
        {"finished": cutoff - timedelta(days=1), "last_old": ids[500]},
    )
    session.execute(
        text(f"UPDATE {prefix}_jobs SET finished_at = :finished WHERE id = :recent"),
        {
            "finished": datetime.now(timezone.utc).replace(tzinfo=None),
            "recent": ids[501],
        },
    )
    session.execute(
        text(f"DELETE FROM {prefix}_ready_executions WHERE job_id <= :recent"),
        {"recent": ids[501]},
    )
    session.commit()

    cutoff_ts = cutoff.replace(tzinfo=timezone.utc).timestamp()
    assert qc.count_clearable_jobs(finished_before=cutoff_ts) == 501
    assert qc.clear_finished_jobs(batch_size=500, finished_before=cutoff_ts) == 501

    assert (
        session.execute(text(f"SELECT count(*) FROM {prefix}_jobs")).scalar_one() == 2
    )
    assert (
        session.execute(
            text(f"SELECT count(*) FROM {prefix}_jobs WHERE finished_at IS NOT NULL")
        ).scalar_one()
        == 1
    )
    assert (
        session.execute(
            text(f"SELECT count(*) FROM {prefix}_ready_executions")
        ).scalar_one()
        == 1
    )
    session.rollback()
