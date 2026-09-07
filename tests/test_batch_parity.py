import json
import sqlite3
from pathlib import Path
from typing import ClassVar

import pytest
import quebec

RAILS_CALLBACK = Path(__file__).parent / "fixtures" / "rails_batch_callback.json"


@pytest.fixture
def env(tmp_path):
    path = tmp_path / "probe.db"
    qc = quebec.Quebec(f"sqlite:///{path}?mode=rwc", table_name_prefix="probe")
    qc.create_tables()
    db = sqlite3.connect(path)
    try:
        yield qc, db
    finally:
        db.close()
        qc.close()


class Member(quebec.BaseClass):
    def perform(self):
        pass


class RailsCallback(quebec.BaseClass):
    def perform(self):
        pass


class AbortCallback(quebec.BaseClass):
    calls: ClassVar[list] = []

    def before_enqueue(self):
        self.calls.append("before")
        raise quebec.AbortEnqueue()

    def perform(self):
        pass


class LimitedCallback(quebec.BaseClass):
    concurrency_limit = 1

    def concurrency_key(self):
        return "one"

    def perform(self):
        pass


def count(db, table):
    return db.execute(f"SELECT COUNT(*) FROM probe_{table}").fetchone()[0]


def test_exception_rolls_back_batch_and_jobs(env):
    qc, db = env
    qc.register_job(Member)
    with pytest.raises(ValueError), qc.batch() as batch:
        Member.perform_later(qc)
        raise ValueError("stop building")
    assert count(db, "jobs") == 0
    assert count(db, "batches") == 0
    assert qc.find_batch(batch.id) is None


def test_nested_enqueue_keeps_outer_batch_open(env):
    qc, _ = env
    qc.register_job(Member)
    with qc.batch() as batch:
        with batch.enqueue():
            pass
        batch.reload()
        assert not batch.finished
        Member.perform_later(qc)


def test_aborting_callback_runs_enqueue_hook(env):
    qc, db = env
    AbortCallback.calls = []
    qc.register_job(AbortCallback)
    with qc.batch(on_finish=AbortCallback) as batch:
        pass
    assert batch.succeeded
    assert AbortCallback.calls == ["before"]
    assert count(db, "jobs") == 0


def test_real_rails_timestamp_is_not_discarded(env):
    qc, db = env
    qc.register_job(RailsCallback)
    batch = qc.batch()
    callback = RAILS_CALLBACK.read_text()
    db.execute(
        "UPDATE probe_batches SET on_finish = ? WHERE id = ?", (callback, batch.id)
    )
    db.commit()
    qc._batch_start(batch.id)
    assert count(db, "scheduled_executions") == 1
    assert count(db, "ready_executions") == 0


def test_rails_callback_applies_registered_concurrency(env):
    qc, db = env
    qc.register_job(LimitedCallback)
    LimitedCallback.perform_later(qc)
    callback = json.loads(RAILS_CALLBACK.read_text())
    callback["job_class"] = "LimitedCallback"
    callback["scheduled_at"] = None
    assert "concurrency_key" not in callback
    batch = qc.batch()
    db.execute(
        "UPDATE probe_batches SET on_finish = ? WHERE id = ?",
        (json.dumps(callback), batch.id),
    )
    db.commit()
    qc._batch_start(batch.id)
    assert count(db, "ready_executions") == 1
    assert count(db, "blocked_executions") == 1


def test_blocked_callback_is_shown_as_blocked(env):
    qc, db = env
    qc.register_job(LimitedCallback)
    LimitedCallback.perform_later(qc)
    with qc.batch(on_finish=LimitedCallback) as batch:
        pass
    assert count(db, "blocked_executions") == 1
    request = quebec.AsgiRequest("GET", f"/batches/{batch.id}", "", [], b"", "")
    status, _, body = qc.handle_control_plane_request(request)
    assert status == 200
    html = bytes(body).decode()
    callback_section = html.split("<!-- Callbacks -->", 1)[1].split(
        "<!-- Metadata -->", 1
    )[0]
    assert "Blocked" in callback_section


def test_outer_rollback_includes_nested_and_bulk_enqueues(env):
    qc, db = env
    qc.register_job(Member)
    with pytest.raises(ValueError), qc.batch() as outer:
        Member.perform_later(qc)
        with qc.batch(on_finish=Member):
            qc.perform_all_later([Member.build(), Member.build()])
        assert qc.find_batch(outer.id).total_jobs == 1
        # Other connections (workers and maintenance) cannot see the work.
        assert count(db, "batches") == 0
        assert count(db, "ready_executions") == 0
        raise ValueError("rollback everything")
    for table in ("jobs", "batches", "batch_executions", "ready_executions"):
        assert count(db, table) == 0


def test_callback_hooks_share_transaction_and_rollback_on_error(env):
    qc, db = env
    calls = []

    class HookCallback(quebec.BaseClass):
        fail = True

        def before_enqueue(self):
            calls.append("before")
            assert self.batch.finished
            Member.perform_later(qc)

        def around_enqueue(self):
            calls.append("around-before")
            yield
            assert self.id is not None
            calls.append("around-after")

        def after_enqueue(self):
            calls.append("after")
            if self.fail:
                raise RuntimeError("hook failed")

        def perform(self):
            pass

    qc.register_job(Member)
    qc.register_job(HookCallback)
    with (
        pytest.raises(RuntimeError, match="hook failed"),
        qc.batch(on_finish=HookCallback) as batch,
    ):
        pass
    assert calls == ["before", "around-before", "around-after", "after"]
    assert count(db, "jobs") == 0  # Includes the enqueue inside before_enqueue.
    assert not batch.reload().finished
    from quebec.context import current_batch_transaction

    assert current_batch_transaction.get() is None
    HookCallback.fail = False
    qc.sweep_stalled_batches()
    assert batch.reload().finished
    assert count(db, "jobs") == 2


def test_callback_concurrency_resolves_batch_and_rails_keywords(env):
    qc, db = env
    performed = []

    class BatchKeyCallback(quebec.BaseClass):
        concurrency_limit = 1

        def concurrency_key(self, *, tenant):
            return f"{self.batch.id}/{tenant}"

        def perform(self, *, tenant):
            performed.append((self.batch.id, tenant))

    qc.register_job(BatchKeyCallback)
    callback = json.loads(RAILS_CALLBACK.read_text())
    callback.update(
        job_class=BatchKeyCallback.__qualname__,
        scheduled_at=None,
        arguments=[{"tenant": "north", "_aj_ruby2_keywords": ["tenant"]}],
    )
    batch = qc.batch()
    db.execute(
        "UPDATE probe_batches SET on_finish = ? WHERE id = ?",
        (json.dumps(callback), batch.id),
    )
    db.commit()
    qc._batch_start(batch.id)
    key = db.execute("SELECT concurrency_key FROM probe_jobs").fetchone()[0]
    assert key.endswith(f"/{batch.id}/north")
    qc.drain_one().perform()
    assert performed == [(batch.id, "north")]


@pytest.mark.parametrize(
    "timestamp, expected",
    [
        ("2035-01-01T12:00:00+02:00", "2035-01-01 10:00:00"),
        ("2035-01-01T12:00:00", "2035-01-01 12:00:00"),
    ],
)
def test_callback_timezone_and_legacy_naive_timestamp(env, timestamp, expected):
    qc, db = env
    qc.register_job(RailsCallback)
    callback = json.loads(RAILS_CALLBACK.read_text())
    callback["scheduled_at"] = timestamp
    batch = qc.batch()
    db.execute(
        "UPDATE probe_batches SET on_finish = ? WHERE id = ?",
        (json.dumps(callback), batch.id),
    )
    db.commit()
    qc._batch_start(batch.id)
    assert (
        db.execute("SELECT scheduled_at FROM probe_jobs")
        .fetchone()[0]
        .startswith(expected)
    )


def test_invalid_callback_timestamp_rolls_back_completion(env):
    qc, db = env
    qc.register_job(RailsCallback)
    callback = json.loads(RAILS_CALLBACK.read_text())
    callback["scheduled_at"] = "not-a-date"
    batch = qc.batch()
    db.execute(
        "UPDATE probe_batches SET on_finish = ? WHERE id = ?",
        (json.dumps(callback), batch.id),
    )
    db.commit()
    with pytest.raises(RuntimeError, match="scheduled_at"):
        qc._batch_start(batch.id)
    assert not batch.reload().finished
    assert count(db, "jobs") == 0


def test_callback_around_hook_receives_constraint_error(env):
    qc, db = env
    seen = []

    class BrokenConstraint(quebec.BaseClass):
        concurrency_limit = 1

        def concurrency_key(self):
            raise ValueError("broken key")

        def around_enqueue(self):
            try:
                yield
            except RuntimeError as error:
                seen.append(str(error))
                raise

        def perform(self):
            pass

    qc.register_job(BrokenConstraint)
    with (
        pytest.raises(RuntimeError, match="broken key"),
        qc.batch(on_finish=BrokenConstraint) as batch,
    ):
        pass
    assert len(seen) == 1 and "broken key" in seen[0]
    assert not batch.reload().finished
    assert count(db, "jobs") == 0


def test_callback_around_hook_receives_database_error(env):
    qc, db = env
    seen = []

    class DatabaseErrorCallback(quebec.BaseClass):
        def around_enqueue(self):
            try:
                yield
            except RuntimeError as error:
                seen.append(str(error))
                raise

        def perform(self):
            pass

    qc.register_job(DatabaseErrorCallback)
    db.execute("""
        CREATE TRIGGER reject_ready BEFORE INSERT ON probe_ready_executions
        BEGIN SELECT RAISE(FAIL, 'injected callback write failure'); END
    """)
    db.commit()
    with (
        pytest.raises(RuntimeError, match="injected callback write failure"),
        qc.batch(on_finish=DatabaseErrorCallback) as batch,
    ):
        pass
    assert len(seen) == 1 and "injected callback write failure" in seen[0]
    assert not batch.reload().finished
    assert count(db, "jobs") == 0
