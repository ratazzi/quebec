"""Live MySQL completion handlers, continuation and batch callbacks."""

import os
import time
import uuid
from contextlib import contextmanager
from datetime import timedelta
from urllib.parse import unquote, urlsplit

import pytest
import quebec


class Exhausted(quebec.BaseClass):
    handler_runs = 0
    retry_on = [
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=60),
            attempts=1,
            handler=lambda job, exc: setattr(
                Exhausted, "handler_runs", Exhausted.handler_runs + 1
            ),
        )
    ]

    def perform(self):
        raise ValueError("exhausted")


class Discarded(quebec.BaseClass):
    handler_runs = 0
    after_runs = 0
    discard_on = [
        quebec.DiscardStrategy(
            (ValueError,),
            lambda job, exc: setattr(
                Discarded, "handler_runs", Discarded.handler_runs + 1
            ),
        )
    ]

    def perform(self):
        raise ValueError("discard")

    def after_discard(self):
        type(self).after_runs += 1


class Rescued(quebec.BaseClass):
    handler_runs = 0
    rescue_from = [
        quebec.RescueStrategy(
            (ValueError,),
            lambda job, exc: setattr(Rescued, "handler_runs", Rescued.handler_runs + 1),
        )
    ]

    def perform(self):
        raise ValueError("rescue")


class ResumeOnce(quebec.BaseClass, quebec.Continuable):
    resumed_runs = 0

    def perform(self):
        with self.step("process", start=0) as step:
            if not step.resumed:
                step.advance(from_=0)
                raise quebec.JobInterrupted("resume once")
            type(self).resumed_runs += 1


class Done(quebec.BaseClass):
    def perform(self, value=None):
        return None


@contextmanager
def mysql_case():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"mfp_{uuid.uuid4().hex[:10]}"
    qc = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    try:
        qc.create_tables()
        qc.register_worker_process()
        yield qc, observer, prefix, dsn
    finally:
        observer.close()
        qc.close()


def count(observer, prefix, table, condition=""):
    with observer.cursor() as cursor:
        cursor.execute(f"SELECT count(*) FROM {prefix}_{table} {condition}")
        return cursor.fetchone()[0]


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_exhausted_discard_and_rescue_handlers_run_once():
    with mysql_case() as (qc, observer, prefix, _dsn):
        for klass in (Exhausted, Discarded, Rescued):
            klass.handler_runs = 0
            qc.register_job(klass)
        Discarded.after_runs = 0
        qc.perform_all_later([Exhausted.build(), Discarded.build(), Rescued.build()])
        executions = qc.drain_batch(3)
        assert len(executions) == 3
        for execution in executions:
            execution.perform()
        assert (
            Exhausted.handler_runs,
            Discarded.handler_runs,
            Discarded.after_runs,
            Rescued.handler_runs,
        ) == (1, 1, 1, 1)
        assert count(observer, prefix, "jobs", "WHERE finished_at IS NOT NULL") == 3
        assert count(observer, prefix, "failed_executions") == 0
        assert count(observer, prefix, "claimed_executions") == 0


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_continuation_resumes_after_dispatch_once():
    with mysql_case() as (qc, observer, prefix, dsn):
        ResumeOnce.resumed_runs = 0
        qc.register_job(ResumeOnce)
        ResumeOnce.perform_later(qc)
        initial = qc.drain_batch(1)
        assert len(initial) == 1
        initial[0].perform()
        assert count(observer, prefix, "scheduled_executions") == 1
        dispatcher = quebec.Quebec(
            dsn,
            table_name_prefix=prefix,
            dispatcher_polling_interval=0.1,
            use_listen_notify=False,
        )
        try:
            dispatcher.spawn_dispatcher()
            deadline = time.perf_counter() + 10
            while count(observer, prefix, "ready_executions") != 1:
                if time.perf_counter() >= deadline:
                    raise TimeoutError("resumed continuation was not promoted")
                time.sleep(0.02)
        finally:
            dispatcher.close()
        resumed = qc.drain_batch(1)
        assert len(resumed) == 1
        resumed[0].perform()
        assert ResumeOnce.resumed_runs == 1
        assert count(observer, prefix, "jobs") == 2
        assert count(observer, prefix, "jobs", "WHERE finished_at IS NOT NULL") == 2
        for table in (
            "ready_executions",
            "scheduled_executions",
            "claimed_executions",
            "failed_executions",
        ):
            assert count(observer, prefix, table) == 0


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_batch_final_job_enqueues_finish_callback():
    with mysql_case() as (qc, observer, prefix, _dsn):
        qc.register_job(Done)
        with qc.batch(on_finish=Done) as batch:
            qc.perform_all_later([Done.build(1), Done.build(2)])
        executions = qc.drain_batch(2)
        assert len(executions) == 2
        executions[0].perform()
        assert not batch.reload().finished
        executions[1].perform()
        assert batch.reload().finished
        assert count(observer, prefix, "ready_executions") == 1
        callback = qc.drain_batch(1)
        assert len(callback) == 1
        callback[0].perform()
        assert count(observer, prefix, "jobs", "WHERE finished_at IS NOT NULL") == 3
        assert count(observer, prefix, "claimed_executions") == 0
