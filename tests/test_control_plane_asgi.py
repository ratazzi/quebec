"""Exercise the ASGI adapter, including event-loop and thread boundaries."""

import asyncio
import contextvars
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import anyio
import pytest
import quebec
from sqlalchemy import text


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request):
    return request.param


async def call_app(
    app,
    *,
    path="/",
    method="GET",
    root_path="",
    chunks=(b"",),
    query=b"",
    headers=(),
    messages=None,
):
    messages = [] if messages is None else messages
    event_loop_thread = threading.get_ident()
    body = iter(enumerate(chunks))

    async def receive():
        assert threading.get_ident() == event_loop_thread
        index, chunk = next(body)
        return {
            "type": "http.request",
            "body": chunk,
            "more_body": index < len(chunks) - 1,
        }

    async def send(message):
        assert threading.get_ident() == event_loop_thread
        messages.append(message)

    await app(
        {
            "type": "http",
            "method": method,
            "path": path,
            "root_path": root_path,
            "query_string": query,
            "headers": list(headers),
        },
        receive,
        send,
    )
    return messages


async def wait_for_thread_event(event):
    with anyio.fail_after(3):
        while not event.is_set():
            await anyio.sleep(0.001)


class BlockingBackend:
    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.thread_id = None

    def handle_control_plane_request(self, request):
        self.thread_id = threading.get_ident()
        self.entered.set()
        # A finite wait makes the pre-fix blocking behavior fail instead of
        # deadlocking the suite before the event loop can release this gate.
        self.release.wait(timeout=1)
        self.finished.set()
        return 200, [], b"ok"


@pytest.mark.anyio
async def test_slow_handler_leaves_event_loop_responsive(anyio_backend):
    backend = BlockingBackend()
    app = quebec.ControlPlaneASGI(backend)
    messages = []

    async def request():
        await call_app(app, messages=messages)

    async with anyio.create_task_group() as group:
        group.start_soon(request)
        try:
            await wait_for_thread_event(backend.entered)
            assert not backend.finished.is_set(), (
                "event loop only resumed after the handler finished"
            )
            assert backend.thread_id != threading.get_ident()
            assert messages == []
        finally:
            backend.release.set()
    assert messages[-1]["body"] == b"ok"


@pytest.mark.anyio
async def test_concurrent_requests_preserve_their_context(anyio_backend):
    request_name = contextvars.ContextVar("request_name", default="missing")
    barrier = threading.Barrier(2, timeout=2)

    class Backend:
        def handle_control_plane_request(self, request):
            barrier.wait()
            name = request_name.get()
            request_name.set("worker-only")
            return 200, [], name.encode()

    app = quebec.ControlPlaneASGI(Backend())
    responses = {}

    async def request(name):
        token = request_name.set(name)
        try:
            messages = await call_app(app)
            responses[name] = messages[-1]["body"]
            assert request_name.get() == name
        finally:
            request_name.reset(token)

    async with anyio.create_task_group() as group:
        group.start_soon(request, "first")
        group.start_soon(request, "second")
    assert responses == {"first": b"first", "second": b"second"}


@pytest.mark.anyio
async def test_cancelled_request_stops_waiting_without_sending(anyio_backend):
    backend = BlockingBackend()
    messages = []
    completed = anyio.Event()

    async def request(*, task_status=anyio.TASK_STATUS_IGNORED):
        try:
            with anyio.CancelScope() as scope:
                task_status.started(scope)
                await call_app(quebec.ControlPlaneASGI(backend), messages=messages)
        finally:
            completed.set()

    async with anyio.create_task_group() as group:
        scope = await group.start(request)
        try:
            await wait_for_thread_event(backend.entered)
            scope.cancel()
            with anyio.fail_after(3):
                await completed.wait()
            assert not backend.finished.is_set()
        finally:
            backend.release.set()
        with anyio.fail_after(3):
            await completed.wait()
    await wait_for_thread_event(backend.finished)
    assert messages == []


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["trio"])
async def test_trio_cancelled_queued_request_never_starts_handler(anyio_backend):
    import trio

    limiter = trio.to_thread.current_default_thread_limiter()
    original_tokens = limiter.total_tokens
    limiter.total_tokens = 1
    calls = []

    class Backend:
        def handle_control_plane_request(self, request):
            calls.append(request)
            return 200, [], b"unexpected"

    try:
        # Occupy the default worker allowance so this request must queue.
        borrower = object()
        await limiter.acquire_on_behalf_of(borrower)
        try:
            with anyio.move_on_after(0.02) as scope:
                await call_app(quebec.ControlPlaneASGI(Backend()))
            assert scope.cancel_called
        finally:
            limiter.release_on_behalf_of(borrower)
        await anyio.lowlevel.checkpoint()
        assert calls == []
    finally:
        limiter.total_tokens = original_tokens


@pytest.mark.anyio
async def test_handler_errors_propagate(anyio_backend):
    class Backend:
        def handle_control_plane_request(self, request):
            raise RuntimeError("handler failed")

    messages = []
    with pytest.raises(RuntimeError, match="handler failed"):
        await call_app(quebec.ControlPlaneASGI(Backend()), messages=messages)
    assert messages == []


@pytest.mark.anyio
async def test_asgi_request_and_response_are_preserved(anyio_backend, monkeypatch):
    requests = []
    original_request = quebec.quebec.AsgiRequest

    def capture_request(*args):
        requests.append(args)
        return original_request(*args)

    monkeypatch.setattr(quebec.quebec, "AsgiRequest", capture_request)

    class Backend:
        def handle_control_plane_request(self, request):
            headers = [(b"location", b"/queues"), (b"x-test", b"1"), (b"x-test", b"2")]
            return 303, headers, b"\x00\xff"

    messages = await call_app(
        quebec.ControlPlaneASGI(Backend()),
        method="POST",
        path="/quebec/queues",
        root_path="/quebec",
        query=b"name=a%2Fb",
        chunks=(b"\x00", b"\xff"),
        headers=((b"x-request", b"value"),),
    )
    assert requests == [
        (
            "POST",
            "/queues",
            "name=a%2Fb",
            [(b"x-request", b"value")],
            b"\x00\xff",
            "/quebec",
        )
    ]
    assert messages == [
        {
            "type": "http.response.start",
            "status": 303,
            "headers": [
                (b"location", b"/quebec/queues"),
                (b"x-test", b"1"),
                (b"x-test", b"2"),
            ],
        },
        {"type": "http.response.body", "body": b"\x00\xff"},
    ]


@pytest.mark.anyio
async def test_sse_keeps_polling_fallback(anyio_backend):
    class Backend:
        def handle_control_plane_request(self, request):
            pytest.fail("SSE must not enter the body-collecting Rust handler")

    messages = await call_app(
        quebec.ControlPlaneASGI(Backend()),
        path="/quebec/events",
        root_path="/quebec",
    )
    assert messages == [
        {"type": "http.response.start", "status": 204, "headers": []},
        {"type": "http.response.body", "body": b""},
    ]


@pytest.mark.anyio
async def test_real_rust_router_accepts_concurrent_asgi_requests(
    anyio_backend,
    qc_with_sqlalchemy,
):
    qc = qc_with_sqlalchemy["qc"]
    app = qc.asgi_app()

    async def pause(name):
        messages = await call_app(
            app,
            method="POST",
            path=f"/quebec/queues/{name}/pause",
            root_path="/quebec",
        )
        assert messages[0]["status"] == 303
        location = dict(messages[0]["headers"])[b"location"]
        assert location == f"/quebec/queues/{name}".encode()
        assert isinstance(messages[-1]["body"], bytes)

    async with anyio.create_task_group() as group:
        group.start_soon(pause, "first")
        group.start_soon(pause, "second")
    rows = (
        qc_with_sqlalchemy["session"]
        .execute(
            text(
                f"SELECT queue_name FROM {qc_with_sqlalchemy['prefix']}_pauses ORDER BY queue_name"
            )
        )
        .scalars()
        .all()
    )
    assert rows == ["first", "second"]
    health = await call_app(app, path="/quebec/health", root_path="/quebec")
    assert health[0]["status"] == 200
    assert json.loads(health[-1]["body"])["jobs"]["ready"] == 0


def test_native_asyncio_cancellation_does_not_send_a_late_response():
    async def scenario():
        backend = BlockingBackend()
        messages = []
        task = asyncio.create_task(
            call_app(
                quebec.ControlPlaneASGI(backend),
                messages=messages,
            )
        )
        try:
            await wait_for_thread_event(backend.entered)
            assert not backend.finished.is_set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not backend.finished.is_set()
        finally:
            backend.release.set()
            await asyncio.gather(task, return_exceptions=True)
        await wait_for_thread_event(backend.finished)
        assert messages == []

    asyncio.run(scenario())


def test_asyncio_cancelled_queued_request_never_starts_handler():
    calls = []
    release = threading.Event()

    class Backend:
        def handle_control_plane_request(self, request):
            calls.append(request)
            return 200, [], b"unexpected"

    async def scenario():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        occupied = loop.run_in_executor(None, release.wait, 2)
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    call_app(quebec.ControlPlaneASGI(Backend())), timeout=0.02
                )
        finally:
            release.set()
            await occupied
        assert calls == []

    asyncio.run(scenario())


@pytest.mark.anyio
async def test_backend_can_be_selected_explicitly(anyio_backend, qc):
    messages = await call_app(qc.asgi_app(backend=anyio_backend), path="/health")
    assert messages[0]["status"] == 200


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_imported_trio_does_not_override_asyncio(anyio_backend, qc):
    import trio

    with pytest.raises(RuntimeError):
        trio.lowlevel.current_task()
    messages = await call_app(qc.asgi_app(), path="/health")
    assert messages[0]["status"] == 200


@pytest.mark.parametrize("backend", ["anyio", "unknown"])
def test_invalid_backend_is_rejected(backend, qc):
    with pytest.raises(ValueError, match="backend must be"):
        qc.asgi_app(backend=backend)


def test_auto_detects_trio_guest_inside_asyncio_host():
    import trio

    class Backend:
        def handle_control_plane_request(self, request):
            return 200, [], b"guest"

    async def host():
        loop = asyncio.get_running_loop()
        done = loop.create_future()

        async def guest():
            # Merely finding a running asyncio loop would pick the wrong
            # backend here: the coroutine is currently being driven by Trio.
            assert asyncio.get_running_loop() is loop
            return await call_app(quebec.ControlPlaneASGI(Backend()))

        trio.lowlevel.start_guest_run(
            guest,
            run_sync_soon_threadsafe=loop.call_soon_threadsafe,
            done_callback=done.set_result,
        )
        outcome = await asyncio.wait_for(done, timeout=3)
        assert outcome.unwrap()[-1]["body"] == b"guest"

    asyncio.run(host())


@pytest.mark.parametrize("mode", ["queue", "asgi", "trio"])
def test_optional_async_libraries_are_not_required(mode):
    script = r"""
import importlib.abc
import sys

class RejectOptionalLibraries(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] == "sniffio" and sys.argv[1] != "trio":
            raise ModuleNotFoundError(name=fullname)
        blocked = {"anyio"} if sys.argv[1] == "trio" else {"anyio", "trio"}
        if fullname.split(".")[0] in blocked:
            raise AssertionError(f"unexpected optional import: {fullname}")

sys.meta_path.insert(0, RejectOptionalLibraries())
import quebec

qc = quebec.Quebec("sqlite::memory:")
qc.create_tables()
try:
    if sys.argv[1] == "queue":
        class Job(quebec.BaseClass):
            def perform(self):
                pass
        qc.register_job(Job)
        assert Job.perform_later(qc).id is not None
        qc.drain_one().perform()
    else:
        async def request():
            messages = []
            async def receive():
                return {"type": "http.request", "body": b""}
            async def send(message):
                messages.append(message)
            await qc.asgi_app()({
                "type": "http", "method": "GET", "path": "/health"
            }, receive, send)
            assert messages[0]["status"] == 200
        if sys.argv[1] == "trio":
            import trio
            trio.run(request)
        else:
            import asyncio
            asyncio.run(request())
finally:
    qc.close()
assert "anyio" not in sys.modules
if sys.argv[1] != "trio":
    assert "trio" not in sys.modules
    assert "sniffio" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script, mode],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
