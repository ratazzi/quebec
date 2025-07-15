# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "quebec",
# ]
#
# [tool.uv.sources]
# quebec = { git = "https://github.com/ratazzi/quebec.git", rev = "master" }
# ///
import sys
import time
import queue
import logging
import threading
from pathlib import Path

FORMAT = '%(asctime)s %(levelname)s [%(name)s:%(filename)s:%(lineno)d]: %(message)s'
logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger()

import quebec # NOQA

event = threading.Event()
db_path = Path(len(sys.argv) > 1 and sys.argv[1] or 'demo.db')
logger.info(db_path)
exists = db_path.exists()
dsn = f'sqlite://{db_path}?mode=rwc'
qc = quebec.Quebec(dsn, use_skip_locked=False)
if not exists:
    qc.create_table()
qc.setup_signal_handler()

@qc.on_shutdown
def handler1():
    event.set()
    logger.info("[DEBUG] shutdown handler1")


@qc.on_start
def start_handler1():
    logger.info("[DEBUG] supervisor start handler1")

@qc.on_worker_start
def worker_start_handler1():
    logger.info("[DEBUG] worker start handler1")


@qc.on_stop
def stop_handler1():
    logger.info("[DEBUG] supervisor stop handler1")


@qc.on_worker_stop
def worker_stop_handler1():
    logger.info("[DEBUG] worker stop handler1")


@qc.register_job
class FakeJob(quebec.BaseClass):
    def perform(self, *args, **kwargs):
        self.logger.info(f">>> {self.id}, args: {args}, kwargs: {kwargs}")
        self.logger.debug("via rust tracing logger")


if __name__ == "__main__":
    job = FakeJob.perform_later(qc, 3466, foo='bar')

    qc.start_control_plane('127.0.0.1:5006')
    qc.spawn_all()

    q = queue.Queue()
    qc.feed_jobs_to_queue(q)
    threading.Thread(target=quebec.ThreadedRunner(q, event).run).start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            logger.debug('KeyboardInterrupt, shutting down...')
            qc.graceful_shutdown()
