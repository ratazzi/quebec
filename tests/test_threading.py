import quebec
import queue
import threading

from .helpers import wait_until


class FakeExecution:
    def __init__(self) -> None:
        self.jid = "job-1"
        self.queue = "default"
        self.module_path = "tests.fake_execution"
        self.tid = None
        self.cleaned_up = threading.Event()
        self.performed = threading.Event()
        self.context = None

    def perform(self) -> None:
        self.context = quebec.job_context_var.get()
        self.performed.set()

    def cleanup(self) -> None:
        self.cleaned_up.set()


class BlockingExecution(FakeExecution):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def perform(self) -> None:
        self.started.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("blocking execution was not released")
        super().perform()


def _join_queue_in_thread(work_queue: queue.Queue) -> tuple[threading.Thread, threading.Event]:
    joined = threading.Event()

    def join_queue() -> None:
        work_queue.join()
        joined.set()

    thread = threading.Thread(target=join_queue)
    thread.start()
    return thread, joined


def test_threaded_runner_processes_execution_and_sets_job_context():
    work_queue = queue.Queue()
    shutdown_event = threading.Event()
    execution = FakeExecution()
    work_queue.put(execution)

    runner = quebec.ThreadedRunner(work_queue, shutdown_event)
    runner_thread = threading.Thread(target=runner.run)
    runner_thread.start()

    try:
        wait_until(
            lambda: execution.performed.is_set() and execution.cleaned_up.is_set(),
            timeout=2,
            message="threaded runner did not process the queued execution",
        )
    finally:
        shutdown_event.set()
        runner_thread.join(timeout=5)

    assert not runner_thread.is_alive()
    assert execution.tid is not None
    assert execution.context.jid == "job-1"
    assert execution.context.queue == "default"
    assert execution.context.target == "tests.fake_execution"


def test_threaded_runner_marks_queue_done_after_perform_returns():
    work_queue = queue.Queue()
    shutdown_event = threading.Event()
    execution = BlockingExecution()
    work_queue.put(execution)

    runner = quebec.ThreadedRunner(work_queue, shutdown_event)
    runner_thread = threading.Thread(target=runner.run)
    runner_thread.start()

    join_thread, joined = _join_queue_in_thread(work_queue)

    try:
        wait_until(
            execution.started.is_set,
            timeout=2,
            message="threaded runner did not start the blocking execution",
        )
        assert not joined.wait(timeout=0.1)

        execution.release.set()
        wait_until(
            joined.is_set,
            timeout=2,
            message="queue.join() did not return after perform completed",
        )
    finally:
        execution.release.set()
        shutdown_event.set()
        runner_thread.join(timeout=5)
        join_thread.join(timeout=5)

    assert not runner_thread.is_alive()
    assert not join_thread.is_alive()
    assert execution.performed.is_set()
    assert execution.cleaned_up.is_set()


def test_threaded_runner_marks_none_sentinel_done():
    work_queue = queue.Queue()
    shutdown_event = threading.Event()
    work_queue.put(None)

    runner = quebec.ThreadedRunner(work_queue, shutdown_event)
    runner_thread = threading.Thread(target=runner.run)
    runner_thread.start()

    join_thread, joined = _join_queue_in_thread(work_queue)

    try:
        wait_until(
            joined.is_set,
            timeout=2,
            message="None sentinel did not decrement queue unfinished task count",
        )
    finally:
        shutdown_event.set()
        runner_thread.join(timeout=5)
        join_thread.join(timeout=5)

    assert not runner_thread.is_alive()
    assert not join_thread.is_alive()
    assert runner.execution is None
