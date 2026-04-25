from .quebec import *  # NOQA
from . import quebec
from . import sqlalchemy  # NOQA
from .quebec import Quebec, ActiveJob, JobInterrupted, InvalidStepError
from .quebec import Continuable as _RustContinuable
from .quebec import StepContext, StepContextManager
import logging
import os
import time
import queue
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Type, Any, Optional, Union, Generator, Callable
from .logger import JobContext, job_context_var

__doc__ = quebec.__doc__
if hasattr(quebec, "__all__"):
    __all__ = quebec.__all__

logger = logging.getLogger(__name__)


class Continuable:
    """Mixin class for jobs with resumable steps.

    Jobs that inherit from this mixin can define resumable steps using the
    `step()` context manager. If a job is interrupted (e.g., during shutdown),
    it will resume from the last checkpoint when re-executed.

    Example:
        class ProcessImportJob(BaseClass, Continuable):
            def perform(self, import_id):
                import_obj = Import.find(import_id)

                with self.step("initialize"):
                    import_obj.initialize()

                with self.step("process", start=0) as step:
                    for record in import_obj.records[step.cursor:]:
                        record.process()
                        step.advance(from_=record.id)  # Auto-checkpoints!

                with self.step("finalize"):
                    import_obj.finalize()

    Step API (matches Rails ActiveJob::Continuation::Step):
        - step.cursor: Get current cursor value (None or start value on first run)
        - step.resumed: Whether this step was resumed from a previous execution
        - step.advanced: Whether the cursor has been advanced during this execution
        - step.set(value): Set cursor and auto-checkpoint (like Rails Step#set!)
        - step.advance(from_=value): Advance cursor (value + 1) and auto-checkpoint (like Rails Step#advance!)
        - step.checkpoint(): Manually check if job should be interrupted

    Step parameters (matches Rails):
        - start: Initial cursor value (e.g., step("foo", start=0))
        - isolated: Run step in its own execution (e.g., step("slow", isolated=True))

    Class attributes (like Rails):
        - max_resumptions: Maximum times a job can be resumed (default None = unlimited)
        - resume_errors_after_advancing: Whether to resume on errors if progress made (default True)

    IMPORTANT: `set()` and `advance()` automatically call `checkpoint()` after
    updating the cursor - matching Rails' Solid Queue behavior. This means you
    don't need to call `checkpoint()` manually after every advance.

    The continuation state is automatically serialized to the job arguments and
    restored when the job is resumed. The `resumptions` property indicates how
    many times the job has been resumed.
    """

    # This will be set by the Rust runtime when the job is executed
    _continuation: Optional[_RustContinuable] = None

    # Class-level configuration (like Rails' class attributes)
    max_resumptions: Optional[int] = None  # None means unlimited
    resume_errors_after_advancing: bool = True

    def step(
        self,
        name: str,
        callback: Optional[Callable] = None,
        *,
        start: Any = None,
        isolated: bool = False,
    ) -> Union[StepContextManager, None]:
        """Create a resumable step.

        Can be used in two ways:

        1. Context manager style (for complex steps with cursor):
            ```python
            with self.step("process", start=0) as step:
                for i in range(step.cursor, 100):
                    do_work(i)
                    step.advance(from_=i)
            ```

        2. Callback style (for simple steps, more like Rails):
            ```python
            self.step("validate", self.do_validate)
            self.step("prepare", lambda: prepare_data())
            ```

        Args:
            name: Unique name for this step within the job
            callback: Optional callable to execute. If provided, the callback
                     is executed only if the step hasn't been completed yet.
                     Can accept an optional step argument for cursor access.
            start: Initial cursor value (like Rails' `start:` parameter).
                   Used when cursor is None (first run or no saved cursor).
            isolated: If True, ensures this step runs in its own execution.
                     Useful for long-running steps that can't checkpoint within
                     the job grace period (like Rails' `isolated: true`).

        Returns:
            If callback is None: A context manager that provides a StepContext
            If callback is provided: None (callback is executed immediately)

        Raises:
            InvalidStepError: If step validation fails (nested, repeated, wrong order)
        """
        if self._continuation is None:
            # Create a default continuation context if not set by runtime
            # This allows testing without the full runtime
            self._continuation = _RustContinuable()
        return self._continuation.step(name, callback, start=start, isolated=isolated)

    @property
    def resumptions(self) -> int:
        """Number of times this job has been resumed from a checkpoint."""
        if self._continuation is None:
            return 0
        return self._continuation.resumptions


class JobDescriptor:
    """Lightweight descriptor holding all data needed to enqueue a job.

    Created via BaseClass.build() or JobBuilder.build(). Does not touch the
    database – all Python-side preparation is done eagerly so that the Rust
    bulk-insert path can release the GIL as early as possible.
    """

    __slots__ = ("job_class", "args", "kwargs", "options")

    def __init__(self, job_class: Type, args: tuple, kwargs: dict, options: dict):
        self.job_class = job_class
        self.args = args
        self.kwargs = kwargs
        self.options = options  # queue, priority, wait, wait_until


class JobBuilder:
    """Builder for configuring job options before enqueueing.

    This allows chaining configuration like:
        MyJob.set(wait=3600).perform_later(arg1, arg2)
        MyJob.set(queue='high', priority=10).perform_later(arg1)

    The Quebec instance is inferred from the class binding set by
    ``@qc.register_job`` / ``qc.register_job_class`` / ``qc.discover_jobs``.
    If the same job class is registered to more than one Quebec instance,
    the most recent registration wins; pass the target instance explicitly
    as the first positional argument to ``perform_later`` to disambiguate.
    """

    def __init__(self, job_class: Type, **options):
        self.job_class = job_class
        self.options = options

    def _calculate_scheduled_at(self) -> Optional[datetime]:
        """Calculate scheduled_at from wait or wait_until options."""
        wait = self.options.get("wait")
        wait_until = self.options.get("wait_until")

        if wait_until is not None:
            if isinstance(wait_until, datetime):
                # Ensure timezone-aware for correct timestamp conversion
                if wait_until.tzinfo is None:
                    # Assume naive datetime is UTC
                    wait_until = wait_until.replace(tzinfo=timezone.utc)
                return wait_until
            raise ValueError("wait_until must be a datetime object")

        if wait is not None:
            # Use timezone-aware UTC datetime
            now = datetime.now(timezone.utc)
            if isinstance(wait, (int, float)):
                return now + timedelta(seconds=wait)
            elif isinstance(wait, timedelta):
                return now + wait
            raise ValueError("wait must be a number (seconds) or timedelta")

        return None

    def build(self, *args, **kwargs) -> "JobDescriptor":
        """Create a JobDescriptor with configured options (for bulk enqueue)."""
        return JobDescriptor(self.job_class, args, kwargs, dict(self.options))

    def perform_later(self, *args, **kwargs) -> "ActiveJob":
        """Enqueue the job with configured options.

        The Quebec instance may be passed explicitly as the first
        positional argument (legacy form) or omitted entirely when the
        job class has been registered with a Quebec instance. A leading
        argument is only consumed as the Quebec instance when it is
        actually a ``Quebec`` object; any other value (including
        ``None``) is treated as job payload data.
        """
        qc = None
        if args and isinstance(args[0], Quebec):
            qc = args[0]
            args = args[1:]

        scheduled_at = self._calculate_scheduled_at()

        # Pass internal options via kwargs (will be filtered out before serialization)
        if scheduled_at is not None:
            kwargs["_scheduled_at"] = scheduled_at.timestamp()

        if "queue" in self.options:
            kwargs["_queue"] = self.options["queue"]

        if "priority" in self.options:
            kwargs["_priority"] = self.options["priority"]

        if qc is None:
            qc = getattr(self.job_class, "quebec", None)
            if qc is None:
                name = self.job_class.__qualname__
                raise TypeError(
                    f"{name} is not registered with a Quebec instance. Use "
                    f"@qc.register_job, qc.register_job_class({name}), or "
                    f"qc.discover_jobs(...) — or pass the Quebec instance as "
                    f"the first argument to perform_later."
                )
        return self.job_class.perform_later(qc, *args, **kwargs)


class NoNewOverrideMeta(type):
    def __new__(cls, name, bases, dct):
        if "__new__" in dct:
            raise TypeError(f"Overriding __new__ is not allowed in class {name}")
        if "__init__" in dct:
            raise TypeError(f"Overriding __init__ is not allowed in class {name}")
        return super().__new__(cls, name, bases, dct)


class BaseClass(ActiveJob, metaclass=NoNewOverrideMeta):
    @classmethod
    def build(cls, *args, **kwargs) -> "JobDescriptor":
        """Create a JobDescriptor for bulk enqueue via perform_all_later.

        Example:
            jobs = [MyJob.build(i) for i in range(10000)]
            qc.perform_all_later(jobs)
        """
        return JobDescriptor(cls, args, kwargs, {})

    @classmethod
    def set(
        cls,
        wait: Union[int, float, timedelta] = None,
        wait_until: datetime = None,
        queue: str = None,
        priority: int = None,
    ) -> JobBuilder:
        """Configure job options before enqueueing.

        Args:
            wait: Delay in seconds (int/float) or timedelta before running
            wait_until: Specific datetime when the job should run
            queue: Queue name to enqueue the job to
            priority: Job priority (lower number = higher priority)

        Returns:
            JobBuilder instance for chaining with perform_later

        Example:
            MyJob.set(wait=3600).perform_later(arg1)  # Run in 1 hour
            MyJob.set(wait_until=tomorrow).perform_later(arg1)
            MyJob.set(queue='critical', priority=1).perform_later(arg1)
        """
        options = {}
        if wait is not None:
            options["wait"] = wait
        if wait_until is not None:
            options["wait_until"] = wait_until
        if queue is not None:
            options["queue"] = queue
        if priority is not None:
            options["priority"] = priority
        return JobBuilder(cls, **options)


class ThreadedRunner:
    def __init__(self, queue: queue.Queue, event: threading.Event):
        self.queue = queue
        self.event = event
        self.execution: Optional[Any] = None

    def run(self):
        """Main loop for processing jobs"""
        while not self.event.is_set():
            try:
                self.execution = self.queue.get(timeout=0.1)
                if self.execution is None:
                    continue

                self.queue.task_done()
                self.execution.tid = str(threading.get_ident())

                # Inject jid and queue into context before execution, clean up after
                ctx_token = job_context_var.set(
                    JobContext(
                        jid=self.execution.jid,
                        queue=self.execution.queue,
                        target=self.execution.module_path,
                    )
                )
                try:
                    self.execution.perform()
                    logger.debug(self.execution.metric)
                finally:
                    job_context_var.reset(ctx_token)
            except queue.Empty:
                pass  # No job available, just continue waiting
            except (
                getattr(queue, "ShutDown", None) or KeyboardInterrupt,
                KeyboardInterrupt,
            ):
                break
            except Exception as e:
                logger.error(
                    f"Unexpected exception in ThreadedRunner: {e}", exc_info=True
                )
            finally:
                self.cleanup()

        logger.debug("threaded_runner exit")

    def cleanup(self):
        """Cleanup after job execution"""
        try:
            if self.execution and hasattr(self.execution, "cleanup"):
                self.execution.cleanup()
        except Exception as e:
            logger.error(f"Error in cleanup: {e}", exc_info=True)


# Runtime state for Quebec instances (PyO3 classes don't support dynamic attributes)
_quebec_state: dict = {}


def _quebec_start(
    self,
    *,
    create_tables: bool = False,
    control_plane: Optional[str] = None,
    spawn: Optional[List[str]] = None,
):
    """Non-blocking start. Returns immediately after all components are started.

    Worker thread count is normally configured via queue.yml
    (workers.threads). The worker_threads constructor parameter can be used
    as an explicit override. There is a single source of truth on the Rust
    side; Python reads it via self.worker_threads.

    Args:
        create_tables: Whether to create database tables (default False).
                       Set to True only if the current user has DDL permissions.
        control_plane: Control plane listen address, e.g. '127.0.0.1:5006'.
        spawn: List of components to spawn. Options: 'worker', 'dispatcher', 'scheduler'.
               None means spawn all components.

    Example:
        qc.start()
        # ... do other work ...
        qc.wait()  # Block until shutdown
    """
    if create_tables:
        self.create_tables()

    self.setup_signal_handler()

    if control_plane:
        self.start_control_plane(control_plane)

    # Thread count comes from Rust (typically queue.yml, optionally constructor override)
    threads = self.worker_threads

    # Spawn components based on spawn parameter
    if spawn is None:
        self.spawn_all()
    else:
        for component in spawn:
            if component == "worker":
                self.spawn_job_claim_poller()
            elif component == "dispatcher":
                self.spawn_dispatcher()
            elif component == "scheduler":
                self.spawn_scheduler()
            else:
                raise ValueError(f"Unknown component: {component}")

    # Set up threading infrastructure
    shutdown_event = threading.Event()
    job_queue = queue.Queue()

    if threads > 0:
        self.bind_queue(job_queue)

    def run_worker():
        runner = ThreadedRunner(job_queue, shutdown_event)
        runner.run()

    # Start worker threads as daemon so program can exit after start()
    worker_threads = []
    for i in range(threads):
        t = threading.Thread(target=run_worker, name=f"quebec-worker-{i}", daemon=True)
        t.start()
        worker_threads.append(t)

    # Store state by instance id
    _quebec_state[id(self)] = {
        "shutdown_event": shutdown_event,
        "job_queue": job_queue,
        "worker_threads": worker_threads,
    }

    return self  # Enable chaining: qc.start().wait()


def _quebec_wait(self):
    """Block until shutdown signal is received.

    Call this after start() to wait for graceful shutdown.

    Example:
        qc.start()
        # ... do other work ...
        qc.wait()
    """
    state = _quebec_state.get(id(self))
    if state is None:
        raise RuntimeError("Quebec not started. Call start() first.")

    try:
        while not state["shutdown_event"].is_set():
            # Detect supervisor death in fork-based mode: if Rust flagged us
            # orphaned, unblock this wait loop so run() returns and the child
            # can exit. `is_orphaned()` stays False unless `watch_parent_pid`
            # was called, so the non-supervisor case is unaffected.
            if self.is_orphaned():
                logger.warning(
                    "Supervisor went away (ppid changed), shutting down child"
                )
                state["shutdown_event"].set()
                self.graceful_shutdown()
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.debug("KeyboardInterrupt, shutting down...")
        self.graceful_shutdown()
    finally:
        # Wait for worker threads to finish
        for t in state["worker_threads"]:
            t.join(timeout=5.0)
        _quebec_state.pop(id(self), None)


def _quebec_run(
    self,
    *,
    create_tables: bool = False,
    control_plane: Optional[str] = None,
    spawn: Optional[List[str]] = None,
    processes: Optional[dict] = None,
):
    """Blocking run. Starts all components and waits until shutdown.

    This is equivalent to calling start() followed by wait().
    Worker thread count is normally configured via queue.yml
    (workers.threads). The worker_threads constructor parameter can be used
    as an explicit override.

    Args:
        create_tables: Whether to create database tables (default False).
                       Set to True only if the current user has DDL permissions.
        control_plane: Control plane listen address, e.g. '127.0.0.1:5006'.
        spawn: List of components to spawn. Options: 'worker', 'dispatcher', 'scheduler'.
               None means spawn all components. Ignored when ``processes`` is set.
        processes: If set, run in fork-based supervisor mode. Accepts a dict
                   like ``{"worker": 2, "dispatcher": 1, "scheduler": 1}``.
                   When unset, the call stays in single-process threaded mode
                   for backward compatibility, even if ``queue.yml`` declares
                   ``processes: >1``. Set ``QUEBEC_SUPERVISOR=1`` to opt in to
                   auto-deriving the plan from ``queue.yml``.

    Example:
        qc.run()  # Start all components and block (single-process mode)
        qc.run(create_tables=True)  # Create tables and start all
        qc.run(spawn=['worker'])  # Only start worker
        qc.run(processes={"worker": 4, "dispatcher": 1})  # Supervisor mode
    """
    plan = processes
    # Auto-deriving the plan from queue.yml is opt-in (QUEBEC_SUPERVISOR=1) so
    # that an existing deployment with `processes: >1` does not silently switch
    # from the single-process threaded runtime to fork-based supervision.
    if plan is None and os.environ.get("QUEBEC_SUPERVISOR") == "1":
        try:
            plan = self.supervisor_plan_from_config()
        except Exception:
            plan = None

    if plan:
        from .supervisor import Supervisor

        if create_tables:
            self.create_tables()
        sup = Supervisor(self, plan, control_plane=control_plane)
        sup.start()
        return

    self.start(
        create_tables=create_tables,
        control_plane=control_plane,
        spawn=spawn,
    )
    self.wait()


class ControlPlaneASGI:
    """ASGI application bridging to Quebec's Rust control plane."""

    def __init__(self, quebec_instance):
        self.qc = quebec_instance

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return

        # Read request body
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break

        # Strip mount prefix (root_path) from path for the Axum router
        path = scope["path"]
        root_path = scope.get("root_path", "")
        if root_path and path.startswith(root_path):
            path = path[len(root_path) :] or "/"

        # SSE (/events) is not supported through the ASGI bridge because
        # handle_request collects the full body, which blocks forever on
        # infinite streams.  Reject with 204 so the frontend JS falls back
        # to polling /stats automatically.
        if path == "/events":
            await send(
                {
                    "type": "http.response.start",
                    "status": 204,
                    "headers": [],
                }
            )
            await send({"type": "http.response.body", "body": b""})
            return

        # Call Rust handler (releases GIL internally via py.detach)
        # base_path is passed to Rust so templates generate correct prefixed URLs
        req = quebec.AsgiRequest(
            scope["method"],
            path,
            (scope.get("query_string") or b"").decode("latin-1"),
            list(scope.get("headers", [])),
            body,
            root_path,
        )
        status, headers, response_body = self.qc.handle_control_plane_request(req)

        # Rewrite Location headers for redirects (303 etc.)
        if root_path:
            prefix = root_path.encode()
            rewritten = []
            for name, value in headers:
                if name.lower() == b"location" and value.startswith(b"/"):
                    value = prefix + value
                rewritten.append((name, value))
            headers = rewritten

        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": headers,
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": response_body,
            }
        )


def _quebec_asgi_app(self):
    """Return an ASGI application for the control plane.

    Mount this on a FastAPI/Starlette app to serve the control plane dashboard.

    Example:
        app = FastAPI()
        qc = Quebec("postgres://localhost/mydb")
        app.mount("/quebec", qc.asgi_app())
    """
    return ControlPlaneASGI(self)


def _quebec_discover_jobs(
    self,
    *packages: str,
    recursive: bool = True,
    on_error: str = "raise",
) -> List[Type]:
    """Import the given packages and register every ``BaseClass`` subclass found.

    This is a thin wrapper around ``importlib`` + ``pkgutil`` that replaces the
    hand-rolled job-discovery loops most projects end up writing (scan a
    ``jobs`` package, ``issubclass(..., BaseClass)``, call ``register_job``).

    A class is only registered once, even if it is re-exported from multiple
    modules. Classes whose ``__module__`` does not fall under one of the given
    packages are ignored, so ``from some.lib import JobMixin`` will not leak
    unrelated classes into the worker registry.

    The worker registry is currently keyed by each class's ``__qualname__``, so
    two jobs sharing a qualified name (e.g. ``foo.jobs.CleanupJob`` and
    ``bar.jobs.CleanupJob``) would silently clobber each other. ``discover_jobs``
    raises ``ValueError`` on such collisions rather than letting them through.

    Args:
        *packages: Dotted package paths to scan (e.g. ``"app.services.jobs"``).
        recursive: When ``True`` (default), walk nested subpackages as well.
        on_error: How to handle import failures in submodules. ``"raise"``
            (default) re-raises the first ``ImportError``; ``"warn"`` emits a
            ``RuntimeWarning`` and continues with the remaining modules. The
            top-level package is always imported strictly — if the root fails
            to import, there is nothing to discover.

    Returns:
        The list of ``BaseClass`` subclasses that were registered, in discovery
        order.

    Raises:
        ValueError: If no packages are given, if ``on_error`` is not one of the
            supported values, or if two discovered classes collide on
            ``__qualname__``.
        ImportError: Propagated from submodule imports when ``on_error="raise"``.

    Example:
        qc = quebec.Quebec(dsn)
        qc.discover_jobs("app.services.jobs")
        qc.run()
    """
    import importlib
    import pkgutil
    import warnings

    if not packages:
        raise ValueError("discover_jobs() requires at least one package name")
    if on_error not in ("raise", "warn"):
        raise ValueError(
            f"discover_jobs() on_error must be 'raise' or 'warn', got {on_error!r}"
        )

    registered: List[Type] = []
    seen: set = set()
    by_qualname: dict = {}

    def _scan_module(module) -> None:
        for obj in module.__dict__.values():
            if not isinstance(obj, type):
                continue
            if not issubclass(obj, BaseClass) or obj is BaseClass:
                continue
            if obj in seen:
                continue
            if not any(
                obj.__module__ == pkg or obj.__module__.startswith(pkg + ".")
                for pkg in packages
            ):
                continue
            existing = by_qualname.get(obj.__qualname__)
            if existing is not None and existing is not obj:
                raise ValueError(
                    f"discover_jobs() found two classes sharing qualname "
                    f"{obj.__qualname__!r}: {existing.__module__}.{existing.__qualname__} "
                    f"and {obj.__module__}.{obj.__qualname__}. Quebec's worker "
                    "registry is keyed by qualname, so the later registration "
                    "would silently replace the earlier one. Rename one of the "
                    "classes or skip this package."
                )
            seen.add(obj)
            by_qualname[obj.__qualname__] = obj
            self.register_job(obj)
            registered.append(obj)

    def _safe_import(module_name: str):
        try:
            return importlib.import_module(module_name)
        except ImportError as exc:
            if on_error == "raise":
                raise
            warnings.warn(
                f"discover_jobs: skipping {module_name!r} (import failed: {exc})",
                RuntimeWarning,
                stacklevel=2,
            )
            return None

    for pkg_name in packages:
        # Top-level package must import cleanly — we cannot walk a missing package.
        package = importlib.import_module(pkg_name)
        _scan_module(package)

        pkg_path = getattr(package, "__path__", None)
        if pkg_path is None:
            # Plain module, not a package — nothing else to walk.
            continue

        walker = pkgutil.walk_packages if recursive else pkgutil.iter_modules
        for info in walker(pkg_path, prefix=f"{pkg_name}."):
            module_name = info.name
            if any(part.startswith("_") for part in module_name.split(".")):
                continue
            submodule = _safe_import(module_name)
            if submodule is None:
                continue
            _scan_module(submodule)

    return registered


# Attach methods to Quebec class
Quebec.start = _quebec_start
Quebec.wait = _quebec_wait
Quebec.run = _quebec_run
Quebec.asgi_app = _quebec_asgi_app
Quebec.discover_jobs = _quebec_discover_jobs
