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
