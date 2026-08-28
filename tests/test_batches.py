"""Solid Queue-compatible batches: membership, counters, completion, callbacks.

Mirrors the cases in Solid Queue's ``test/integration/batch_lifecycle_test.rb``.
The worker is driven synchronously (``drain_one`` + ``perform``); scheduled
retries are promoted to ready with a direct SQL move so no dispatcher thread
is involved.
"""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
import quebec
from quebec.context import current_batch_id
from sqlalchemy import text


class Record(quebec.BaseClass):
    values: list = []

    def perform(self, value) -> None:
        Record.values.append(value)


class FailFast(quebec.BaseClass):
    def perform(self) -> None:
        raise ValueError("fail fast")


class Retrying(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy((ValueError,), wait=timedelta(0), attempts=3, handler=None)
    ]

    def perform(self) -> None:
        raise ValueError("retry me")


class SucceedsOnSecondAttempt(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy((ValueError,), wait=timedelta(0), attempts=3, handler=None)
    ]

    def perform(self) -> None:
        if self.executions == 0:
            raise ValueError("first attempt")
        Record.values.append("second attempt ok")


class Discarding(quebec.BaseClass):
    discard_on = [quebec.DiscardStrategy((ValueError,), lambda job, exc: None)]

    def perform(self) -> None:
        raise ValueError("discard me")


class DiscardOnConflict(quebec.BaseClass):
    concurrency_limit = 1
    concurrency_duration = 60
    concurrency_on_conflict = quebec.ConcurrencyConflict.Discard

    def concurrency_key(self, *args, **kwargs) -> str:
        return "shared"

    def perform(self, value) -> None:
        Record.values.append(value)


class OnFinish(quebec.BaseClass):
    def perform(self, tag: str = "") -> None:
        b = self.batch
        Record.values.append(f"finish {tag}#{b.id}: {b.total_jobs} total")


class OnSuccess(quebec.BaseClass):
    def perform(self, tag: str = "") -> None:
        b = self.batch
        Record.values.append(f"{tag}: {b.completed_jobs} jobs succeeded!")


class OnFailure(quebec.BaseClass):
    def perform(self, tag: str = "") -> None:
        b = self.batch
        Record.values.append(f"{tag}: {b.failed_jobs} jobs failed!")


class AddsMoreJobs(quebec.BaseClass):
    def perform(self) -> None:
        qc = type(self).quebec
        with self.batch.enqueue():
            Record.perform_later(qc, "added from inside 1")
            Record.perform_later(qc, "added from inside 2")
            with qc.batch():
                Record.perform_later(qc, "added from inside 3")


ALL_JOBS = [
    Record,
    FailFast,
    Retrying,
    SucceedsOnSecondAttempt,
    Discarding,
    DiscardOnConflict,
    OnFinish,
    OnSuccess,
    OnFailure,
    AddsMoreJobs,
]


@pytest.fixture
def env(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    Record.values = []
    for klass in ALL_JOBS:
        qc.register_job(klass)
    return qc_with_sqlalchemy


def _drain(qc) -> int:
    """Run every claimable job (promoting scheduled retries) until none are left."""
    ran = 0
    while True:
        try:
            execution = qc.drain_one()
        except RuntimeError as exc:
            if "No job found" not in str(exc):
                raise
            return ran
        execution.perform()
        ran += 1


def _promote_scheduled(env) -> int:
    """Move scheduled executions to ready, as the dispatcher would."""
    session, prefix = env["session"], env["prefix"]
    session.execute(
        text(
            f"INSERT INTO {prefix}_ready_executions (job_id, queue_name, priority, created_at) "
            f"SELECT job_id, queue_name, priority, created_at FROM {prefix}_scheduled_executions"
        )
    )
    moved = session.execute(text(f"DELETE FROM {prefix}_scheduled_executions")).rowcount
    session.commit()
    return moved


def _run_until_settled(env) -> None:
    qc = env["qc"]
    while True:
        _drain(qc)
        if _promote_scheduled(env) == 0:
            return


def _count(env, table: str, where: str = "") -> int:
    env["session"].expire_all()
    sql = f"SELECT COUNT(*) FROM {env['prefix']}_{table} {where}"
    return env["session"].execute(text(sql)).scalar()


def _jobs(env, batch_id: int | None = None) -> list[dict]:
    env["session"].expire_all()
    where = f"WHERE batch_id = {batch_id}" if batch_id is not None else ""
    rows = env["session"].execute(
        text(f"SELECT * FROM {env['prefix']}_jobs {where} ORDER BY id")
    )
    return [dict(r._mapping) for r in rows]


def _parse(ts) -> datetime:
    return ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts))


def test_schema_created_and_idempotent(env) -> None:
    qc, session, prefix = env["qc"], env["session"], env["prefix"]
    assert qc.create_tables() is True  # second run is a no-op
    cols = {r[1] for r in session.execute(text(f"PRAGMA table_info({prefix}_jobs)"))}
    assert "batch_id" in cols
    assert _count(env, "batches") == 0
    assert _count(env, "batch_executions") == 0


def test_batch_id_added_to_legacy_jobs_table(env) -> None:
    session, prefix = env["session"], env["prefix"]
    session.execute(text(f"DROP INDEX IF EXISTS idx_{prefix}_jobs_batch_id"))
    session.execute(text(f"ALTER TABLE {prefix}_jobs DROP COLUMN batch_id"))
    session.commit()
    assert env["qc"].create_tables() is True
    cols = {r[1] for r in session.execute(text(f"PRAGMA table_info({prefix}_jobs)"))}
    assert "batch_id" in cols


def test_empty_batches_fire_callbacks(env) -> None:
    qc = env["qc"]
    with qc.batch(on_success=OnSuccess.build("3")):
        with qc.batch(on_success=OnSuccess.build("2")):
            with qc.batch(on_success=OnSuccess.build("1")):
                pass
            with qc.batch(on_success=OnSuccess.build("1.1")):
                pass

    assert _count(env, "batches", "WHERE finished_at IS NOT NULL") == 4
    _drain(qc)
    assert sorted(Record.values) == [
        "1.1: 0 jobs succeeded!",
        "1: 0 jobs succeeded!",
        "2: 0 jobs succeeded!",
        "3: 0 jobs succeeded!",
    ]


def test_members_and_jobs_enqueued_from_inside_a_job(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch1:
        job1 = Record.perform_later(qc, "hey")
        with qc.batch() as batch2:
            job2 = Record.perform_later(qc, "ho")
            job3 = AddsMoreJobs.perform_later(qc)

    assert batch1.status == "enqueued"
    assert batch1.total_jobs == 1
    assert batch2.total_jobs == 2
    assert _count(env, "batch_executions") == 3

    _drain(qc)

    assert sorted(Record.values) == [
        "added from inside 1",
        "added from inside 2",
        "added from inside 3",
        "hey",
        "ho",
    ]
    assert _count(env, "batches", "WHERE finished_at IS NOT NULL") == 3
    assert _count(env, "batch_executions") == 0

    batch1.reload()
    batch2.reload()
    assert batch1.succeeded and batch2.succeeded
    assert batch2.total_jobs == 4  # ho, AddsMoreJobs, and the two added from inside
    assert batch1.completed_jobs == 1

    finished = {j["id"]: _parse(j["finished_at"]) for j in _jobs(env)}
    assert finished[job3.id] <= _parse(batch2.finished_at)
    assert finished[job2.id] <= _parse(batch2.finished_at)
    assert finished[job1.id] <= _parse(batch1.finished_at)


def test_prebuilt_descriptor_captures_batch(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        Record.perform_later(qc, "keeper")  # keeps the batch open
        descriptor = Record.build("prebuilt")
        assert descriptor.options["batch_id"] == batch.id
    assert current_batch_id.get() is None

    # Enqueued after the block: still a member.
    (job,) = qc.perform_all_later([descriptor])
    assert job.batch_id == batch.id
    batch.reload()
    assert batch.total_jobs == 2
    assert batch.pending_jobs == 2

    _drain(qc)
    batch.reload()
    assert batch.succeeded
    assert sorted(Record.values) == ["keeper", "prebuilt"]


def test_prebuilt_descriptor_does_not_keep_batch_open(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        descriptor = Record.build("too late")
    assert batch.finished  # nothing was enqueued, so it finished on exit
    with pytest.raises(quebec.BatchAlreadyFinished):
        qc.perform_all_later([descriptor])
    assert _count(env, "jobs") == 0


def test_open_batch_wins_over_descriptor_batch(env) -> None:
    qc = env["qc"]
    with qc.batch() as outer:
        descriptor = Record.build("x")
    with qc.batch() as inner:
        (job,) = qc.perform_all_later([descriptor])
    assert job.batch_id == inner.id
    assert inner.reload().total_jobs == 1
    assert outer.reload().total_jobs == 0


def test_failed_jobs_fire_on_failure(env) -> None:
    qc = env["qc"]
    with qc.batch(on_failure=OnFailure.build("0")) as batch1:
        Retrying.perform_later(qc)
        with qc.batch(on_failure=OnFailure.build("1")) as batch2:
            Retrying.perform_later(qc)

    _run_until_settled(env)

    for batch in (batch1, batch2):
        batch.reload()
        assert batch.status == "failed"
        assert batch.failed
        assert batch.total_jobs == 1  # 1 logical job despite 2 retries
        assert batch.failed_jobs == 1
        assert batch.completed_jobs == 0
        assert batch.pending_jobs == 0
        assert len(_jobs(env, batch.id)) == 3  # each attempt is its own row

    assert sorted(Record.values) == ["0: 1 jobs failed!", "1: 1 jobs failed!"]


def test_retry_then_succeed_counts_once(env) -> None:
    qc = env["qc"]
    with qc.batch(on_success=OnSuccess.build("ok")) as batch:
        SucceedsOnSecondAttempt.perform_later(qc)
        Record.perform_later(qc, "hey")

    _run_until_settled(env)

    batch.reload()
    assert batch.succeeded
    assert batch.total_jobs == 2
    assert batch.completed_jobs == 2
    assert batch.failed_jobs == 0
    assert len(_jobs(env, batch.id)) == 3
    assert "ok: 2 jobs succeeded!" in Record.values
    assert "second attempt ok" in Record.values


def test_perform_all_later_members(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch1:
        qc.perform_all_later([Retrying.build(), Retrying.build()])
        with qc.batch() as batch2:
            qc.perform_all_later([Record.build("ok"), Record.build("ok2")])

    assert batch1.total_jobs == 2
    assert batch2.total_jobs == 2

    _run_until_settled(env)

    batch1.reload()
    batch2.reload()
    assert len(_jobs(env, batch1.id)) == 6
    assert batch1.total_jobs == 2
    assert batch1.failed
    assert len(_jobs(env, batch2.id)) == 2
    assert batch2.succeeded
    assert _count(env, "batches", "WHERE finished_at IS NOT NULL") == 2


def test_discarded_jobs_count_as_completed(env) -> None:
    qc = env["qc"]
    with qc.batch(on_success=OnSuccess.build("0")) as batch1:
        Discarding.perform_later(qc)
        with qc.batch(on_success=OnSuccess.build("1")) as batch2:
            Discarding.perform_later(qc)

    _drain(qc)

    for batch in (batch1, batch2):
        batch.reload()
        assert batch.succeeded
        assert batch.total_jobs == 1
        assert batch.failed_jobs == 0
        assert batch.completed_jobs == 1
        assert batch.pending_jobs == 0
    assert sorted(Record.values) == ["0: 1 jobs succeeded!", "1: 1 jobs succeeded!"]


def test_concurrency_discard_on_enqueue_counts_as_completed(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        DiscardOnConflict.perform_later(qc, "first")
        DiscardOnConflict.perform_later(qc, "second")  # discarded on enqueue

    assert batch.total_jobs == 2
    assert batch.pending_jobs == 1
    assert batch.completed_jobs == 1
    assert not batch.finished

    _drain(qc)
    batch.reload()
    assert batch.succeeded
    assert batch.completed_jobs == 2
    assert Record.values == ["first"]


def test_clearing_finished_jobs_keeps_batch(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        Record.perform_later(qc, "hey")
    _drain(qc)

    batch.reload()
    assert batch.succeeded
    assert qc.clear_finished_jobs(finished_before=4102444800.0) == 1
    assert _count(env, "jobs") == 0
    batch.reload()
    assert batch.succeeded
    assert batch.completed_jobs == 1


def test_batch_interface(env) -> None:
    qc = env["qc"]
    with qc.batch(
        description="Process user imports",
        on_finish=OnFinish,
        on_success=OnSuccess,
        on_failure=OnFailure,
        metadata={"source": "test"},
        user_id=123,
    ) as batch:
        Record.perform_later(qc, "hey")

    assert batch.description == "Process user imports"
    assert batch.metadata == {"source": "test", "user_id": 123}
    assert batch.active_job_batch_id
    assert batch.status == "enqueued"
    assert batch.progress_percentage == 0.0
    assert repr(batch).startswith("<Batch id=")

    _drain(qc)

    batch.reload()
    assert batch.completed_jobs == 1
    assert batch.failed_jobs == 0
    assert batch.pending_jobs == 0
    assert batch.total_jobs == 1
    assert batch.progress_percentage == 100.0
    assert sorted(Record.values) == sorted(
        [f"finish #{batch.id}: 1 total", ": 1 jobs succeeded!", "hey"]
    )

    callbacks = [j for j in _jobs(env) if j["class_name"] in ("OnFinish", "OnSuccess")]
    assert len(callbacks) == 2
    for row in callbacks:
        assert row["batch_id"] is None  # callbacks are not members
        assert json.loads(row["arguments"])["callback_batch_id"] == batch.id
    assert not any(j["class_name"] == "OnFailure" for j in _jobs(env))


def test_member_job_sees_its_batch(env) -> None:
    qc = env["qc"]
    seen = {}

    class Inspect(quebec.BaseClass):
        def perform(self) -> None:
            seen["batch_id"] = self.batch_id
            seen["batch"] = self.batch

    qc.register_job(Inspect)
    with qc.batch(description="inspect") as batch:
        Inspect.perform_later(qc)
    _drain(qc)
    assert seen["batch_id"] == batch.id
    assert seen["batch"].description == "inspect"


def test_enqueue_after_finish_raises(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        Record.perform_later(qc, "one")
    _drain(qc)
    assert batch.reload().finished

    with pytest.raises(quebec.BatchAlreadyFinished):
        with batch.enqueue():
            Record.perform_later(qc, "late")

    # A stale context (the batch finished underneath) is refused atomically:
    # no jobs row is left behind.
    before = _count(env, "jobs")
    token = current_batch_id.set(batch.id)
    try:
        with pytest.raises(quebec.BatchAlreadyFinished):
            Record.perform_later(qc, "late")
        with pytest.raises(quebec.BatchAlreadyFinished):
            qc.perform_all_later([Record.build("late"), Record.build("later")])
    finally:
        current_batch_id.reset(token)
    assert _count(env, "jobs") == before


def test_add_jobs_to_running_batch(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        Record.perform_later(qc, "first")
    with batch.enqueue():
        Record.perform_later(qc, "second")
    assert batch.total_jobs == 2
    assert batch.pending_jobs == 2
    _drain(qc)
    assert batch.reload().succeeded
    assert sorted(Record.values) == ["first", "second"]


def test_nested_context_is_restored(env) -> None:
    qc = env["qc"]
    assert current_batch_id.get() is None
    with qc.batch() as outer:
        assert current_batch_id.get() == outer.id
        with qc.batch() as inner:
            assert current_batch_id.get() == inner.id
        assert current_batch_id.get() == outer.id
    assert current_batch_id.get() is None


def test_exception_inside_block_still_starts_batch(env) -> None:
    qc = env["qc"]
    with pytest.raises(RuntimeError, match="boom"):
        with qc.batch() as batch:
            Record.perform_later(qc, "before boom")
            raise RuntimeError("boom")
    assert current_batch_id.get() is None
    assert batch.status == "enqueued"
    _drain(qc)
    assert batch.reload().succeeded


def test_callback_options_are_honoured(env) -> None:
    qc = env["qc"]
    with qc.batch(
        on_success=OnSuccess.set(queue="batches", priority=5).build("q"),
        on_finish=OnFinish.set(wait=3600).build("later"),
    ):
        pass

    rows = {j["class_name"]: j for j in _jobs(env)}
    assert rows["OnSuccess"]["queue_name"] == "batches"
    assert rows["OnSuccess"]["priority"] == 5
    assert _count(env, "ready_executions") == 1
    assert _count(env, "scheduled_executions") == 1
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    assert _parse(rows["OnFinish"]["scheduled_at"]) > now + timedelta(minutes=30)


def test_batches_unavailable_on_legacy_schema(env) -> None:
    qc, session, prefix, db_url = (
        env["qc"],
        env["session"],
        env["prefix"],
        env["db_url"],
    )
    session.execute(text(f"DROP TABLE {prefix}_batch_executions"))
    session.execute(text(f"DROP TABLE {prefix}_batches"))
    session.execute(text(f"DROP INDEX IF EXISTS idx_{prefix}_jobs_batch_id"))
    session.execute(text(f"ALTER TABLE {prefix}_jobs DROP COLUMN batch_id"))
    session.commit()

    with pytest.raises(RuntimeError, match="batches schema is not installed"):
        qc.batch()
    with pytest.raises(RuntimeError, match="batches schema is not installed"):
        qc.find_batch(1)

    # Plain jobs keep working end to end without the column.
    Record.perform_later(qc, "legacy")
    _drain(qc)
    assert Record.values == ["legacy"]
    assert _count(env, "jobs", "WHERE finished_at IS NOT NULL") == 1

    # A fresh connection without the column still reads job rows.
    con = sqlite3.connect(db_url.split("///", 1)[1].split("?")[0])
    assert "batch_id" not in {
        r[1] for r in con.execute(f"PRAGMA table_info({prefix}_jobs)")
    }
    con.close()


def test_find_batch_by_uuid_and_progress(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        Record.perform_later(qc, "a")
        Record.perform_later(qc, "b")

    found = qc.find_batch_by_active_job_batch_id(batch.active_job_batch_id)
    assert found is not None and found.id == batch.id
    assert qc.find_batch(batch.id + 1000) is None
    assert qc.find_batch_by_active_job_batch_id("nope") is None

    qc.drain_one().perform()
    batch.reload()
    assert batch.pending_jobs == 1
    assert batch.completed_jobs == 1
    assert batch.progress_percentage == 50.0
    assert not batch.finished

    _drain(qc)
    assert batch.reload().progress_percentage == 100.0


def test_manual_retry_does_not_rejoin_batch(env) -> None:
    qc = env["qc"]
    with qc.batch(on_failure=OnFailure.build("x")) as batch:
        job = FailFast.perform_later(qc)
    _drain(qc)

    batch.reload()
    assert batch.failed
    assert batch.failed_jobs == 1
    assert _count(env, "failed_executions") == 1

    assert qc.retry_failed(job.id) is True
    assert _count(env, "batch_executions") == 0
    _drain(qc)  # fails again, records a new failed execution
    batch.reload()
    assert batch.failed
    assert batch.total_jobs == 1
    assert batch.failed_jobs == 1


def test_discard_failed_releases_batch(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        FailFast.perform_later(qc)
        Record.perform_later(qc, "ok")
    _drain(qc)
    batch.reload()
    assert batch.failed
    # A failed job's tracking row was already released when it failed; discarding
    # it later is a no-op for the batch.
    assert qc.discard_all_failed() == 1
    assert _count(env, "batch_executions") == 0
    assert batch.reload().failed


def test_cancel_scheduled_releases_batch(env) -> None:
    qc = env["qc"]
    with qc.batch() as batch:
        scheduled = Record.set(wait=3600).perform_later(qc, "later")
        DiscardOnConflict.perform_later(qc, "holder")
    assert batch.pending_jobs == 2

    assert qc.cancel_scheduled(scheduled.id) is True
    assert batch.reload().pending_jobs == 1
    assert not batch.finished

    _drain(qc)
    assert batch.reload().succeeded
    assert batch.completed_jobs == 2  # cancelled counts as completed, like discard


def test_parallel_finish_has_single_winner(env) -> None:
    qc = env["qc"]
    with qc.batch(on_finish=OnFinish.build("once")) as batch:
        for i in range(20):
            Record.perform_later(qc, i)

    executions = qc.drain_batch(20)
    assert len(executions) == 20
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda e: e.perform(), executions))

    assert batch.reload().succeeded
    assert batch.completed_jobs == 20
    assert _count(env, "jobs", "WHERE class_name = 'OnFinish'") == 1
    _drain(qc)
    assert Record.values.count(f"finish once#{batch.id}: 20 total") == 1
