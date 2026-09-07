"""After-commit recovery and batch use by maintenance-only processes."""

import pytest
import quebec
from quebec.context import current_batch_id, current_batch_transaction
from sqlalchemy import text


class Member(quebec.BaseClass):
    def perform(self):
        pass


class DiscardMember(quebec.BaseClass):
    concurrency_limit = 1
    concurrency_on_conflict = quebec.ConcurrencyConflict.Discard

    def concurrency_key(self):
        return "shared"

    def perform(self):
        pass


def test_start_errors_do_not_skip_other_starts(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    failed = []

    class BrokenCallback(quebec.BaseClass):
        def after_enqueue(self):
            failed.append(self.active_job_id)
            raise RuntimeError("broken callback")

        def perform(self):
            pass

    qc.register_job(BrokenCallback)
    qc.register_job(Member)
    with pytest.raises(RuntimeError) as caught, qc.batch() as outer:
        with qc.batch(on_finish=BrokenCallback) as first:
            pass
        with qc.batch(on_finish=BrokenCallback) as second:
            pass
        with qc.batch(on_finish=Member) as good:
            pass

    assert len(failed) == 2
    assert f"start batch {first.id}" in str(caught.value)
    assert f"start batch {second.id}" in str(caught.value)
    assert good.reload().finished
    assert outer.reload().finished
    assert not first.reload().finished
    assert not second.reload().finished


def test_start_errors_do_not_skip_released_batches(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class BrokenCallback(quebec.BaseClass):
        def after_enqueue(self):
            raise RuntimeError("broken callback")

        def perform(self):
            pass

    for cls in (BrokenCallback, Member, DiscardMember):
        qc.register_job(cls)
    DiscardMember.perform_later(qc)  # Occupy the semaphore outside a batch.
    bad = qc._batch_create(on_finish=BrokenCallback)
    good = qc._batch_create(on_finish=Member)
    session.execute(
        text(f"UPDATE {prefix}_batches SET enqueued_at = CURRENT_TIMESTAMP")
    )
    session.commit()

    # Exercise the released list without also registering these batches in
    # starts: discard-on-conflict releases their only new tracking row.
    transaction, owner = qc._batch_begin()
    assert owner
    token = current_batch_transaction.set(transaction)
    try:
        pending = qc._batch_create(on_finish=BrokenCallback)
        qc._batch_start(pending.id)
        for batch in (bad, good):
            batch_token = current_batch_id.set(batch.id)
            try:
                DiscardMember.perform_later(qc)
            finally:
                current_batch_id.reset(batch_token)
    finally:
        current_batch_transaction.reset(token)
    with pytest.raises(RuntimeError) as caught:
        qc._batch_end(transaction, True)

    assert f"start batch {pending.id}" in str(caught.value)
    assert f"finish batch {bad.id}" in str(caught.value)
    assert qc.find_batch(good.id).finished
    assert not qc.find_batch(bad.id).finished


@pytest.mark.parametrize("finish_via", ["sweep", "supervisor"])
def test_unregistered_maintenance_process_enqueues_callback(
    qc_with_sqlalchemy, finish_via
):
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]
    performed = []
    hooks = []

    class Callback(quebec.BaseClass):
        def before_enqueue(self):
            hooks.append("before")

        def perform(self):
            performed.append(self.batch.id)

    qc.register_job(Member)
    qc.register_job(Callback)
    with qc.batch(on_finish=Callback) as batch:
        job = Member.perform_later(qc)
    maintenance = quebec.Quebec(qc_with_sqlalchemy["db_url"], table_name_prefix=prefix)
    try:
        if finish_via == "sweep":
            session.execute(
                text(
                    f"UPDATE {prefix}_jobs SET finished_at = CURRENT_TIMESTAMP WHERE id = :id"
                ),
                {"id": job.id},
            )
            session.execute(text(f"DELETE FROM {prefix}_ready_executions"))
            session.commit()
            maintenance.sweep_stalled_batches()
        else:
            process_id = qc.register_worker_process()
            claimed = qc.drain_one()
            maintenance.supervisor_fail_claimed_by_process_id(process_id)
            del claimed
        assert batch.reload().finished
        assert hooks == []  # No class registry in the maintenance process.
        qc.drain_one().perform()
        assert performed == [batch.id]
    finally:
        maintenance.close()


def test_caught_inner_error_keeps_writes_in_outer_transaction(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(Member)
    with qc.batch() as outer:
        with pytest.raises(ValueError), qc.batch() as inner:
            Member.perform_later(qc)
            raise ValueError("caught by the outer body")
        Member.perform_later(qc)
    assert inner.reload().total_jobs == 1
    assert inner.status == "pending"  # No start registration after a failed block.
    assert outer.reload().total_jobs == 1
    assert outer.enqueued
    with inner.enqueue():
        pass
    assert inner.enqueued


def test_start_registration_error_always_rolls_back_owner(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(Member)
    ended = []

    class FaultyStart:
        def __getattr__(self, name):
            return getattr(qc, name)

        def _batch_start(self, batch_id):
            raise ValueError("start registration failed")

        def _batch_end(self, transaction, commit):
            ended.append(commit)
            return qc._batch_end(transaction, commit)

    batch = qc.batch()
    batch._qc = FaultyStart()
    with pytest.raises(ValueError, match="start registration failed"), batch:
        Member.perform_later(qc)
    assert ended == [False]
    assert current_batch_transaction.get() is None
    assert qc.find_batch(batch.id) is None


def test_enqueue_uses_cached_transaction_contextvar(qc_with_sqlalchemy, monkeypatch):
    import quebec.context

    qc = qc_with_sqlalchemy["qc"]
    qc.register_job(Member)
    Member.perform_later(qc)  # Initialize the native cache.

    class UnexpectedLookup:
        def get(self):
            pytest.fail("native enqueue should reuse the original ContextVar")

    monkeypatch.setattr(quebec.context, "current_batch_transaction", UnexpectedLookup())
    monkeypatch.setattr(quebec.context, "current_batch_id", UnexpectedLookup())
    with qc.batch() as batch:
        Member.perform_later(qc)
    assert batch.total_jobs == 1
