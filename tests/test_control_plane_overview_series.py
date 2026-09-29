"""Overview chart bucket boundaries on both supported test databases."""

import html
import json
import re
from datetime import datetime, timedelta, timezone

import quebec
from sqlalchemy import text


class OverviewWork(quebec.BaseClass):
    def perform(self, value: int) -> None:
        pass


class OverviewPerfWork(quebec.BaseClass):
    queue_as = "perf"

    def perform(self, value: int) -> None:
        pass


class OverviewOtherWork(quebec.BaseClass):
    queue_as = "other"

    def perform(self, value: int) -> None:
        pass


def chart_counts(qc, hours: int) -> list[int]:
    request = quebec.AsgiRequest("GET", "/", f"hours={hours}", [], b"", "/quebec")
    status, _headers, body = qc.handle_control_plane_request(request)
    assert status == 200
    match = re.search(r"data-jobs-processed-data='([^']*)'", bytes(body).decode())
    assert match is not None
    return json.loads(html.unescape(match.group(1)))


def test_overview_chart_counts_distributed_finished_jobs(qc_with_sqlalchemy) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]
    qc.register_job(OverviewWork)
    qc.perform_all_later([OverviewWork.build(index) for index in range(3)])

    job_ids = (
        session.execute(text(f"SELECT id FROM {prefix}_jobs ORDER BY id"))
        .scalars()
        .all()
    )
    assert len(job_ids) == 3
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for job_id, hours_ago in zip(job_ids, (3, 10, 200), strict=True):
        finished = now - timedelta(hours=hours_ago)
        session.execute(
            text(
                f"UPDATE {prefix}_jobs SET created_at = :created, "
                "finished_at = :finished WHERE id = :id"
            ),
            {
                "created": finished - timedelta(seconds=1),
                "finished": finished,
                "id": job_id,
            },
        )
    session.execute(text(f"DELETE FROM {prefix}_ready_executions"))
    session.commit()

    day = chart_counts(qc, 24)
    week = chart_counts(qc, 168)
    month = chart_counts(qc, 720)
    assert len(day) == 24
    assert sum(day) == 2
    assert sum(value > 0 for value in day) == 2
    assert len(week) == 28
    assert sum(week) == 2
    assert sum(value > 0 for value in week) == 2
    assert len(month) == 30
    assert sum(month) == 3
    assert sorted(value for value in month if value) == [1, 2]


def test_overview_queue_failure_rates_keep_queue_and_time_boundaries(
    qc_with_sqlalchemy,
) -> None:
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]
    qc.register_job(OverviewPerfWork)
    qc.register_job(OverviewOtherWork)
    qc.perform_all_later(
        [OverviewPerfWork.build(index) for index in range(4)]
        + [OverviewOtherWork.build(index) for index in range(2)]
    )
    rows = session.execute(
        text(f"SELECT id, queue_name FROM {prefix}_jobs ORDER BY id")
    ).all()
    ids = {"perf": [], "other": []}
    for job_id, queue_name in rows:
        ids[queue_name].append(job_id)
    assert len(ids["perf"]) == 4 and len(ids["other"]) == 2
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for job_id in ids["perf"][:2] + ids["other"][:1]:
        session.execute(
            text(f"UPDATE {prefix}_jobs SET finished_at = :now WHERE id = :id"),
            {"now": now, "id": job_id},
        )
        session.execute(
            text(f"DELETE FROM {prefix}_ready_executions WHERE job_id = :id"),
            {"id": job_id},
        )
    old_id = ids["perf"][3]
    session.execute(
        text(f"UPDATE {prefix}_jobs SET created_at = :created WHERE id = :id"),
        {"created": now - timedelta(days=31), "id": old_id},
    )
    for job_id in (ids["perf"][0], old_id, ids["other"][1]):
        session.execute(
            text(
                f"INSERT INTO {prefix}_failed_executions (job_id, error, created_at) "
                "VALUES (:id, :error, :now)"
            ),
            {"id": job_id, "error": "fixture failure", "now": now},
        )
    session.commit()

    request = quebec.AsgiRequest("GET", "/", "hours=24", [], b"", "/quebec")
    status, _headers, body = qc.handle_control_plane_request(request)
    assert status == 200
    rendered = bytes(body).decode()
    for name, processed, failed_rate in (("perf", 2, 33), ("other", 1, 50)):
        match = re.search(
            rf">{name}</a>\s*</td>\s*<td[^>]*>(\d+)</td>\s*"
            rf"<td[^>]*>[^<]*</td>\s*<td[^>]*>(\d+)%</td>",
            rendered,
        )
        assert match is not None, f"missing queue performance row for {name}"
        assert (int(match[1]), int(match[2])) == (processed, failed_rate)
