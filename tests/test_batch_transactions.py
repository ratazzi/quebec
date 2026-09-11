"""Transaction boundaries, on SQLite and optionally TEST_POSTGRESQL_URL."""

import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
import quebec


@pytest.fixture(
    params=["sqlite"] + (["postgres"] if os.getenv("TEST_POSTGRESQL_URL") else [])
)
def transactional_qc(request, temp_db_path, test_prefix):
    url = (
        os.environ["TEST_POSTGRESQL_URL"]
        if request.param == "postgres"
        else f"sqlite:///{temp_db_path}?mode=rwc"
    )
    qc = quebec.Quebec(url, table_name_prefix=test_prefix)
    qc.create_tables()
    yield qc, request.param
    qc.close()


class Work(quebec.BaseClass):
    def perform(self):
        pass


def test_uncommitted_batches_are_invisible_to_other_contexts(
    transactional_qc, temp_db_path, test_prefix
):
    qc, backend = transactional_qc
    qc.register_job(Work)
    with qc.batch() as batch:
        Work.perform_later(qc)
        assert qc.find_batch(batch.id).total_jobs == 1
        if backend == "sqlite":
            # Quebec's SQLite pool has one connection, held by the batch.
            # Use an independent reader rather than waiting for that pool.
            with sqlite3.connect(temp_db_path) as reader:
                assert (
                    reader.execute(
                        f"SELECT COUNT(*) FROM {test_prefix}_batches"
                    ).fetchone()[0]
                    == 0
                )
        else:
            with ThreadPoolExecutor(max_workers=1) as pool:
                assert pool.submit(qc.find_batch, batch.id).result(timeout=3) is None
    assert qc.find_batch(batch.id).pending_jobs == 1
    qc.drain_one().perform()
    assert batch.reload().succeeded


def test_rollback_of_existing_batch_keeps_original_members(transactional_qc):
    qc, _ = transactional_qc
    qc.register_job(Work)
    with qc.batch() as batch:
        Work.perform_later(qc)
    with pytest.raises(ValueError), batch.enqueue():
        qc.perform_all_later([Work.build(), Work.build()])
        raise ValueError("rollback additions")
    assert batch.reload().total_jobs == 1
    assert batch.pending_jobs == 1
    qc.drain_one().perform()
    assert batch.reload().succeeded


def test_nested_creation_rolls_back_with_outer_context(transactional_qc):
    qc, _ = transactional_qc
    qc.register_job(Work)
    with pytest.raises(ValueError), qc.batch() as outer:
        with qc.batch() as inner:
            Work.perform_later(qc)
        assert not inner.finished
        raise ValueError("abort parent")
    assert qc.find_batch(outer.id) is None
    assert qc.find_batch(inner.id) is None
    with pytest.raises(RuntimeError, match="No ready job"):
        qc.drain_one()


def test_parallel_postgres_batch_contexts_are_isolated(transactional_qc):
    qc, backend = transactional_qc
    if backend != "postgres":
        pytest.skip("SQLite intentionally serializes writers")
    qc.register_job(Work)
    barrier = threading.Barrier(2, timeout=3)

    def enqueue():
        with qc.batch() as batch:
            Work.perform_later(qc)
            barrier.wait()
        return batch.id

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(enqueue)
        second = pool.submit(enqueue)
        ids = [first.result(timeout=5), second.result(timeout=5)]
    assert ids[0] != ids[1]
    assert [qc.find_batch(id).total_jobs for id in ids] == [1, 1]


def test_callback_failure_rolls_back_hook_enqueues(transactional_qc):
    qc, _ = transactional_qc
    qc.register_job(Work)

    class Callback(quebec.BaseClass):
        def before_enqueue(self):
            assert self.batch.finished
            Work.perform_later(qc)

        def after_enqueue(self):
            raise RuntimeError("abort callback transaction")

        def perform(self):
            pass

    qc.register_job(Callback)
    with (
        pytest.raises(RuntimeError, match="abort callback transaction"),
        qc.batch(on_finish=Callback) as batch,
    ):
        pass
    assert not batch.reload().finished
    assert qc.drain_batch(10) == []


def test_parallel_completion_enqueues_callback_once(transactional_qc):
    qc, _ = transactional_qc
    qc.register_job(Work)
    with qc.batch(on_finish=Work) as batch:
        qc.perform_all_later([Work.build() for _ in range(8)])
    executions = qc.drain_batch(8)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda execution: execution.perform(), executions))
    assert batch.reload().completed_jobs == 8
    callbacks = qc.drain_batch(8)
    assert len(callbacks) == 1
    callbacks[0].perform()


def test_batch_object_can_be_reused_after_rollback(transactional_qc):
    qc, _ = transactional_qc
    qc.register_job(Work)
    batch = qc.batch()
    with pytest.raises(ValueError), batch:
        Work.perform_later(qc)
        raise ValueError("rollback")
    with batch:
        Work.perform_later(qc)
    assert batch.total_jobs == 1
