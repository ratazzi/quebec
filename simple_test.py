import os
import sys
import time
import signal
import queue
import logging
import traceback
import psycopg2
from pathlib import Path
from psycopg2 import sql
import maturin_import_hook; maturin_import_hook.install()
from datetime import datetime, timezone, timedelta

from sqlalchemy import create_engine, text, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from utils import ISO8601Formatter

# FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
FORMAT = '%(asctime)s %(levelname)s [%(name)s:%(filename)s:%(lineno)d]: %(message)s'

formatter = ISO8601Formatter(FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

import quebec
import threading
import asyncio
# from quebec import rescue_from

logger.debug(quebec)
logger.debug(dir(quebec))
# quebec.run_with_signal_handling()

dsn = 'postgres://ratazzi:@localhost:5432/helvetica_development?application_name=quebec'
if len(sys.argv) > 1:
    db_path = Path(sys.argv[1])
    logger.info(db_path)
    dsn = f'sqlite://{db_path}?mode=rwc'

# dsn = 'sqlite::memory:'
qc = q = quebec.Quebec(dsn,
    use_skip_locked=True,
    dispatcher_polling_interval=timedelta(milliseconds=30000),
    dispatcher_concurrency_maintenance_interval=timedelta(milliseconds=30000),
    worker_polling_interval=timedelta(milliseconds=30000)
)
# if not db_path.exists():
qc.create_table()
if 'memory' in dsn:
    qc.create_table()
# q2 = quebec.Quebec(dsn)
event = threading.Event()

def signal_handler(sig, frame):
    signal_names = {signal.SIGINT: 'SIGINT', signal.SIGTERM: 'SIGTERM', signal.SIGQUIT: 'SIGQUIT', signal.SIGABRT: 'SIGABRT', signal.SIGKILL: 'SIGKILL'}
    s = signal_names.get(sig, sig)
    logger.debug(f'{s} received, shutting down...')
    # qc.graceful_shutdown()
    event.set()
    th = threading.Thread(target=qc.graceful_shutdown)
    th.start()
    th.join()
    time.sleep(1)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGQUIT, signal_handler)

# qc.setup_signal_handler()

# @qc.on_shutdown
# def handler1():
#     logger.info("[DEBUG] shutdown handler1")


@qc.register_job
class FakeJob(quebec.BaseClass):
    queue_as = 'default'  # 修改为 default 以匹配 LISTEN 频道
    concurrency_limit = 1
    concurrency_duration = 120 # seconds

    retry_on = [
        quebec.RetryStrategy((RuntimeError,), wait=timedelta(seconds=10), attempts=1, handler=lambda exc: True),
    ]

    def concurrency_key(self, *args, **kwargs):
        return f"FakeJob/{args[0]}"

    def on_exception(self, exc):
        logger.error(f"**************** handler2 handler form python: {repr(self)} {type(exc)} {exc}")
        # qc.graceful_shutdown()

    # @quebec.BaseClass.rescue_from(RuntimeError)
    def handle_runtime_error(self, exc, *args, **kwargs):
        # print(globals())
        print(f"handle_runtime_error: {repr(self)} {type(exc)} {exc}")

    async def async_perform(self, *args, **kwargs):
        url = "postgresql+asyncpg://ratazzi:@localhost:5432/helvetica_development"
        engine = create_async_engine(url)

        async with engine.connect() as conn:
            query = "SELECT * FROM users LIMIT 1"
            result = await conn.execute(text(query))
            logger.debug(str(result.fetchall()[0][0:4]))

        await engine.dispose()

    def perform(self, *args, **kwargs):
        self.logger.info(f">>> {self.id}, args: {args}, kwargs: {kwargs}")

        # return
        # time.sleep(1)
        # raise Exception("test exception")
        # raise RuntimeError("test exception")
        # return asyncio.run(self.async_perform(*args, **kwargs))

        # url = "postgresql+psycopg2://ratazzi:@localhost:5432/helvetica_development"
        url = "postgresql://ratazzi:@localhost:5432/helvetica_development"
        conn_params = psycopg2.extensions.parse_dsn(url)

        # engine = create_engine(url)
        # self.logger.debug(str(engine))
        # 直接执行 SQL 查询

        def execute_sql(query):
            # with engine.connect() as conn:
            # result = conn.execute(text(query))
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()

            # 执行查询
            cursor.execute(query)
            rows = cursor.fetchall()  # 获取所有结果

            # rows = result.fetchall()  # 获取所有结果
            # engine.dispose()

            return rows[0]

        # 示例调用
        # conn.close()
        # self.logger.debug(str(execute_sql("SELECT * FROM users LIMIT 1")[0:4]))

        # time.sleep(0.01)
        # time.sleep(1)
        self.logger.debug("via rust tracing logger")

        # raise quebec.CustomError("test exception")
        # # FBA15JFN313NU000024

    def __repr__(self):
        return f"<FakeJob queue_as={self.queue_as} priority={self.priority}>"


if __name__ == "__main__":
    current_pid = os.getpid()
    logger.info(f"The current process ID is \033[91m{current_pid}\033[0m")
    # qc.register_job_class(FakeJob)

    queued = FakeJob.perform_later(q, 3466, foo='bar')

    qc.spawn_dispatcher()
    qc.spawn_scheduler()
    qc.spawn_job_claim_poller()

    q = queue.Queue()
    threaded_runner = quebec.ThreadedRunner(q, event)

    threads = 1
    if threads > 0:
        qc.feed_jobs_to_queue(q)
    for i in range(threads):
        threading.Thread(target=threaded_runner.run).start()


    def feed_jobs(qc):
        time.sleep(1)
        for i in range(10):
            FakeJob.perform_later(qc, 3466, foo='bar')

    threading.Thread(target=feed_jobs, args=(qc,)).start()

    # app executor
    # qc.wait_for_shutdown()
    while True:
        try:
            # print('--------------- running loop ---------------')
            time.sleep(1)
        except KeyboardInterrupt:
            logger.debug('KeyboardInterrupt, shutting down...')
            sys.exit(0)
