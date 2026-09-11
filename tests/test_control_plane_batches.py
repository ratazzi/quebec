"""Control plane pages for batches: the list, the detail page, the nav entry,
and the batch link on a job's page — with and without the batches schema."""

from __future__ import annotations

import pytest
import quebec
from sqlalchemy import text

BASE = "/quebec"


class Member(quebec.BaseClass):
    def perform(self, value) -> None:
        pass


class Boom(quebec.BaseClass):
    def perform(self) -> None:
        raise RuntimeError("boom")


class Done(quebec.BaseClass):
    def perform(self) -> None:
        pass


@pytest.fixture
def env(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    for klass in (Member, Boom, Done):
        qc.register_job(klass)
    return qc_with_sqlalchemy


def _get(qc, path: str) -> tuple[int, str]:
    req = quebec.AsgiRequest("GET", path, "", [], b"", BASE)
    status, _headers, body = qc.handle_control_plane_request(req)
    return status, bytes(body).decode()


def _drain(qc) -> None:
    while True:
        try:
            execution = qc.drain_one()
        except RuntimeError as exc:
            if "No job found" in str(exc):
                return
            raise
        execution.perform()


def test_list_page_shows_batches_and_progress(env) -> None:
    qc = env["qc"]
    with qc.batch(on_finish=Done) as done:
        Member.perform_later(qc, 3)
    with qc.batch(description="Nightly import", user_id=7) as running:
        Member.perform_later(qc, 1)
        Member.perform_later(qc, 2)
    qc.drain_one().perform()  # `done`'s job (lowest id): finishes it
    qc.drain_one().perform()  # first job of `running`: 50%
    assert done.reload().succeeded
    assert running.reload().status == "enqueued"

    status, body = _get(qc, "/batches")
    assert status == 200
    assert "Nightly import" in body
    assert f"/batches/{running.id}" in body
    assert f"/batches/{done.id}" in body
    assert "1 / 2" in body  # running batch progress
    assert 'id="nav-option-batches"' in body
    assert "Batches · 1" in body  # one running batch in the nav badge

    status, body = _get(qc, "/batches?status=enqueued")
    assert status == 200
    assert f"/batches/{running.id}" in body
    assert f"/batches/{done.id}" not in body

    status, body = _get(qc, "/batches?status=completed")
    assert status == 200
    assert f"/batches/{running.id}" not in body
    assert f"/batches/{done.id}" in body


def test_list_page_clamps_page_number(env) -> None:
    qc = env["qc"]
    with qc.batch():
        pass
    req = quebec.AsgiRequest("GET", "/batches", "page=99", [], b"", BASE)
    status, headers, _body = qc.handle_control_plane_request(req)
    location = next(v.decode() for k, v in headers if k.lower() == b"location")
    assert status in (302, 303)
    assert location == f"{BASE}/batches?page=1"


def test_detail_page_lists_jobs_and_callbacks(env) -> None:
    qc = env["qc"]
    with qc.batch(
        description="Import 42",
        on_success=Done,
        on_failure=Done.set(queue="alerts").build(),
        metadata={"source": "test"},
    ) as batch:
        job = Member.perform_later(qc, "x")
        Boom.perform_later(qc)

    status, body = _get(qc, f"/batches/{batch.id}")
    assert status == 200
    assert "Import 42" in body
    assert f"/jobs/{job.id}" in body
    assert "on_success" in body and "on_failure" in body
    assert "not enqueued yet" in body
    assert "&quot;source&quot;: &quot;test&quot;" in body  # HTML-escaped JSON
    assert "Enqueued" in body  # status badge

    _drain(qc)
    status, body = _get(qc, f"/batches/{batch.id}")
    assert status == 200
    assert "Failed" in body
    callback_job_id = (
        env["session"]
        .execute(
            text(
                f"SELECT id FROM {env['prefix']}_jobs WHERE class_name = 'Done' AND queue_name = 'alerts'"
            )
        )
        .scalar()
    )
    assert f"/jobs/{callback_job_id}" in body  # on_failure was enqueued and linked

    status, _body = _get(qc, "/batches/424242")
    assert status == 404


def test_job_page_links_to_its_batch(env) -> None:
    qc = env["qc"]
    with qc.batch(on_finish=Done) as batch:
        job = Member.perform_later(qc, "x")

    status, body = _get(qc, f"/jobs/{job.id}")
    assert status == 200
    assert f'quebec/batches/{batch.id}"' in body

    _drain(qc)
    callback_job_id = (
        env["session"]
        .execute(text(f"SELECT id FROM {env['prefix']}_jobs WHERE class_name = 'Done'"))
        .scalar()
    )
    status, body = _get(qc, f"/jobs/{callback_job_id}")
    assert status == 200
    assert "Callback of" in body
    assert f'quebec/batches/{batch.id}"' in body


def test_pages_degrade_without_batches_schema(env) -> None:
    qc, session, prefix = env["qc"], env["session"], env["prefix"]
    Member.perform_later(qc, "plain")
    session.execute(text(f"DROP TABLE {prefix}_batch_executions"))
    session.execute(text(f"DROP TABLE {prefix}_batches"))
    session.execute(text(f"DROP INDEX IF EXISTS idx_{prefix}_jobs_batch_id"))
    session.execute(text(f"ALTER TABLE {prefix}_jobs DROP COLUMN batch_id"))
    session.commit()
    with pytest.raises(RuntimeError):
        qc.batch()  # forces a fresh schema probe

    status, body = _get(qc, "/batches")
    assert status == 200
    assert "Batches are not installed" in body
    assert "nav-option-batches" not in body

    status, _body = _get(qc, "/batches/1")
    assert status == 404

    job_id = session.execute(text(f"SELECT id FROM {prefix}_jobs")).scalar()
    status, body = _get(qc, f"/jobs/{job_id}")
    assert status == 200
    assert "/batches/" not in body
