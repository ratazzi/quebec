"""Redirects from the control plane must carry the mount prefix.

When the dashboard is mounted under a sub-path (`/quebec`), the ASGI adapter
strips the prefix before handing the request to the Axum router and passes it
along as `base_path`. Handlers therefore see `/queues/x/pause`, but the
browser must be sent back to `/quebec/queues/x` — a bare `Location` would
leave the mount. These tests call the Rust entry point directly, bypassing
the adapter's own Location rewrite, so they check the handlers themselves.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta

import pytest

import quebec

BASE = "/quebec"


class QuietJob(quebec.BaseClass):
    def perform(self, *args, **kwargs) -> None:
        return None


class FailingJob(quebec.BaseClass):
    def perform(self, *args, **kwargs) -> None:
        raise RuntimeError("boom")


def _request(qc, method: str, path: str, headers: dict[str, str] | None = None):
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    req = quebec.AsgiRequest(method, path, "", raw, b"", BASE)
    status, resp_headers, _body = qc.handle_control_plane_request(req)
    location = next(
        (v.decode() for k, v in resp_headers if k.lower() == b"location"), None
    )
    return status, location


def _scheduled_execution_id(db_url: str, prefix: str) -> int:
    path = db_url.removeprefix("sqlite:///").split("?", 1)[0]
    with sqlite3.connect(path) as conn:
        (execution_id,) = conn.execute(
            f'SELECT id FROM "{prefix}_scheduled_executions"'
        ).fetchone()
    return execution_id


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/queues/reports/pause", f"{BASE}/queues/reports"),
        ("/queues/reports/resume", f"{BASE}/queues/reports"),
        ("/queues/all/pause", f"{BASE}/queues"),
        ("/queues/all/resume", f"{BASE}/queues"),
    ],
)
def test_queue_actions_redirect_within_the_mount(qc, path, expected) -> None:
    status, location = _request(qc, "POST", path)
    assert status == 303
    assert location == expected


def test_scheduled_job_cancel_redirects_within_the_mount(
    qc, db_url, test_prefix
) -> None:
    qc.register_job(QuietJob)
    QuietJob.set(wait=timedelta(hours=1)).perform_later(qc)
    execution_id = _scheduled_execution_id(db_url, test_prefix)

    status, location = _request(qc, "POST", f"/scheduled-jobs/{execution_id}/cancel")
    assert status == 303
    assert location == f"{BASE}/scheduled-jobs"


def test_recurring_run_now_redirects_within_the_mount(qc) -> None:
    # The action redirects even when the row is stale/missing, which is enough
    # to exercise its Location without seeding a recurring schedule.
    status, location = _request(qc, "POST", "/recurring-jobs/999/run")
    assert status == 303
    assert location == f"{BASE}/recurring-jobs"


def test_failed_job_actions_fall_back_within_the_mount(qc) -> None:
    """Without a Referer the fallback path is app-relative and must be
    prefixed; with one, the browser already sent the prefixed URL and it is
    used as-is (minus `page`, for bulk actions)."""
    qc.register_job(FailingJob)

    FailingJob.perform_later(qc)
    qc.drain_one().perform()
    status, location = _request(qc, "POST", "/failed-jobs/all/retry")
    assert status == 303
    assert location == f"{BASE}/failed-jobs"

    FailingJob.perform_later(qc)
    qc.drain_one().perform()
    status, location = _request(
        qc,
        "POST",
        "/failed-jobs/all/delete",
        {"Referer": f"http://example.test{BASE}/failed-jobs?page=3&queue_name=default"},
    )
    assert status == 303
    assert location == f"{BASE}/failed-jobs?queue_name=default"


def test_blocked_job_actions_fall_back_within_the_mount(qc) -> None:
    status, location = _request(qc, "POST", "/blocked-jobs/all/unblock")
    assert status == 303
    assert location == f"{BASE}/blocked-jobs"


@pytest.mark.parametrize(
    "referer",
    [
        "http://example.test/blocked-jobs",
        "https://other.test/blocked-jobs",
        "not a URL",
    ],
)
def test_referer_outside_the_mount_uses_the_prefixed_fallback(
    qc, referer
) -> None:
    status, location = _request(
        qc,
        "POST",
        "/blocked-jobs/all/unblock",
        {"Referer": referer},
    )
    assert status == 303
    assert location == f"{BASE}/blocked-jobs"
