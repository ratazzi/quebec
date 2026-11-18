"""A job gives its concurrency slot back only to the semaphore it took it from.

Solid Queue #827: a job that ran past concurrency_duration saw its semaphore
expire and get deleted. When it finished it gave a slot back by key, to the
semaphore a later job had created, so one more job than the limit ran.

As in Solid Queue 1.7.2, a job records the semaphore id and limit it got its
slot with in its serialized data, and gives the slot back only to that
semaphore. Ported from Solid Queue's concurrency_lock_test.rb, minus the cases
about a missing job class, which quebec handles differently, and about
discarding ready jobs, which quebec does not support.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from sqlalchemy import text

import quebec

LOCK_KEY = "solid_queue_concurrency_lock"


class NonOverlappingJob(quebec.BaseClass):
    queue_as = "default"
    concurrency_limit = 1
    concurrency_duration = 60

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "concurrency-lock-test"

    def perform(self, *args, **kwargs):
        pass


class TwoAtATimeJob(quebec.BaseClass):
    queue_as = "default"
    concurrency_limit = 2
    concurrency_duration = 60

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "concurrency-lock-test"

    def perform(self, *args, **kwargs):
        pass


class FailingOnceJob(quebec.BaseClass):
    queue_as = "default"
    concurrency_limit = 1
    concurrency_duration = 60
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,), wait=timedelta(seconds=0), attempts=3, handler=None
        )
    ]

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "concurrency-lock-test"

    def perform(self, *args, **kwargs):
        raise ValueError("boom")


class AlwaysFailingJob(quebec.BaseClass):
    queue_as = "default"
    concurrency_limit = 1
    concurrency_duration = 60

    @staticmethod
    def concurrency_key(*args, **kwargs) -> str:
        return "concurrency-lock-test"

    def perform(self, *args, **kwargs):
        raise RuntimeError("boom")


@pytest.fixture
def env(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]
    for cls in (NonOverlappingJob, TwoAtATimeJob, FailingOnceJob, AlwaysFailingJob):
        qc.register_job(cls)
    return qc, qc_with_sqlalchemy["session"], qc_with_sqlalchemy["prefix"]


def _rows(session, sql):
    session.expire_all()
    return session.execute(text(sql)).fetchall()


def _where(session, prefix, job_id):
    return [
        t
        for t in ("ready", "blocked", "claimed", "failed", "scheduled")
        if _rows(
            session, f"SELECT 1 FROM {prefix}_{t}_executions WHERE job_id = {job_id}"
        )
    ]


def _semaphore(session, prefix):
    (row,) = _rows(session, f"SELECT id, value FROM {prefix}_semaphores")
    return row.id, row.value


def _lock(session, prefix, job_id):
    (row,) = _rows(session, f"SELECT arguments FROM {prefix}_jobs WHERE id = {job_id}")
    return json.loads(row.arguments).get(LOCK_KEY)


def _expire_semaphores(session, prefix):
    """The dispatcher's sweep deleting semaphores past their expiry."""
    session.execute(text(f"DELETE FROM {prefix}_semaphores"))
    session.commit()


def _claim(qc):
    execution = qc.drain_one()
    assert execution is not None
    return execution


# From the reproduction in https://github.com/rails/solid_queue/issues/827
def test_a_job_that_outlived_its_lock_does_not_release_the_lock_a_later_job_acquired(
    env,
) -> None:
    qc, session, prefix = env
    NonOverlappingJob.perform_later(qc)
    a = _claim(qc)

    _expire_semaphores(session, prefix)

    b = NonOverlappingJob.perform_later(qc)
    c = NonOverlappingJob.perform_later(qc)
    assert _where(session, prefix, c.id) == ["blocked"]

    a.perform()

    assert _where(session, prefix, b.id) == ["ready"]
    assert _where(session, prefix, c.id) == ["blocked"]
    assert _semaphore(session, prefix)[1] == 0


def test_a_job_that_outlived_its_lock_does_not_leave_the_key_over_its_limit(
    env,
) -> None:
    qc, session, prefix = env
    NonOverlappingJob.perform_later(qc)
    a = _claim(qc)

    _expire_semaphores(session, prefix)

    b = NonOverlappingJob.perform_later(qc)
    c = NonOverlappingJob.perform_later(qc)
    d = NonOverlappingJob.perform_later(qc)

    a.perform()
    _claim(qc).perform()  # B, which hands its slot to C
    assert _where(session, prefix, b.id) == []
    assert _where(session, prefix, c.id) == ["ready"]
    _claim(qc)  # C is running

    assert _where(session, prefix, d.id) == ["blocked"]


def test_a_job_dispatched_before_semaphores_were_recorded_gives_back_by_key(
    env,
) -> None:
    qc, session, prefix = env
    a = NonOverlappingJob.perform_later(qc)
    b = NonOverlappingJob.perform_later(qc)
    args = json.loads(
        _rows(session, f"SELECT arguments FROM {prefix}_jobs WHERE id = {a.id}")[0][0]
    )
    args.pop(LOCK_KEY)
    session.execute(
        text(f"UPDATE {prefix}_jobs SET arguments = :a WHERE id = {a.id}"),
        {"a": json.dumps(args)},
    )
    session.commit()

    _claim(qc).perform()

    assert _where(session, prefix, b.id) == ["ready"]


def test_a_blocked_job_records_the_semaphore_it_gets_when_released(env) -> None:
    qc, session, prefix = env
    a = NonOverlappingJob.perform_later(qc)
    b = NonOverlappingJob.perform_later(qc)
    semaphore_id, _ = _semaphore(session, prefix)
    assert _lock(session, prefix, a.id) == {"semaphore_id": semaphore_id, "limit": 1}
    assert _lock(session, prefix, b.id) is None

    _claim(qc).perform()

    assert _where(session, prefix, b.id) == ["ready"]
    assert _lock(session, prefix, b.id) == {"semaphore_id": semaphore_id, "limit": 1}


def test_jobs_enqueued_in_bulk_record_the_semaphore_they_get(env) -> None:
    qc, session, prefix = env
    qc.perform_all_later([NonOverlappingJob.build("A"), NonOverlappingJob.build("B")])
    a, b = (
        row.id
        for row in _rows(
            session,
            f"SELECT id FROM {prefix}_jobs WHERE class_name = 'NonOverlappingJob' "
            "ORDER BY id",
        )
    )
    semaphore_id, _ = _semaphore(session, prefix)

    assert _where(session, prefix, a) == ["ready"]
    assert _lock(session, prefix, a)["semaphore_id"] == semaphore_id
    assert _where(session, prefix, b) == ["blocked"]
    assert _lock(session, prefix, b) is None


def test_jobs_sharing_a_semaphore_give_their_slots_back_to_it(env) -> None:
    qc, session, prefix = env
    a = TwoAtATimeJob.perform_later(qc)
    b = TwoAtATimeJob.perform_later(qc)
    c = TwoAtATimeJob.perform_later(qc)
    semaphore_id, _ = _semaphore(session, prefix)
    for job in (a, b):
        assert _lock(session, prefix, job.id) == {
            "semaphore_id": semaphore_id,
            "limit": 2,
        }
    assert _where(session, prefix, c.id) == ["blocked"]

    first = _claim(qc)
    _claim(qc)
    first.perform()

    assert _where(session, prefix, c.id) == ["ready"]
    assert _semaphore(session, prefix)[1] == 0


def test_a_retried_failed_job_records_the_semaphore_it_gets_on_retry(env) -> None:
    qc, session, prefix = env
    a = AlwaysFailingJob.perform_later(qc)
    _claim(qc).perform()
    assert _where(session, prefix, a.id) == ["failed"]
    first_semaphore_id = _lock(session, prefix, a.id)["semaphore_id"]

    _expire_semaphores(session, prefix)
    assert qc.retry_failed(a.id) is True

    assert _where(session, prefix, a.id) == ["ready"]
    semaphore_id, _ = _semaphore(session, prefix)
    assert semaphore_id != first_semaphore_id
    assert _lock(session, prefix, a.id)["semaphore_id"] == semaphore_id


def test_a_retry_on_retry_does_not_inherit_the_previous_attempts_slot(env) -> None:
    qc, session, prefix = env
    first = FailingOnceJob.perform_later(qc)
    _claim(qc).perform()

    (retry,) = _rows(
        session,
        f"SELECT id FROM {prefix}_jobs WHERE id <> {first.id} "
        "AND class_name = 'FailingOnceJob'",
    )
    # The slot belonged to the first attempt; the retry records its own
    # once it acquires one.
    assert _where(session, prefix, retry.id) == ["scheduled"]
    assert _lock(session, prefix, retry.id) is None
