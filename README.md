# Quebec

Quebec is a simple background task queue for processing asynchronous tasks. The name is derived from the NATO phonetic alphabet for "Q", representing "Queue".

This project is inspired by [Solid Queue](https://github.com/rails/solid_queue).

> **Warning:** This project is in early development stage. Not recommended for production use.

## Why Quebec?

- **Simplified Architecture**: No dependencies on Redis or message queues
- **Database-Powered**: Leverages RDBMS capabilities for complex task queries and management
- **Rust Implementation**: High performance and safety with Python compatibility
- **Framework Agnostic**: Works with asyncio, Trio, threading, SQLAlchemy, Django, FastAPI, etc.

## Features

- Scheduled tasks
- Recurring tasks
- Concurrency control
- Web dashboard
- Automatic retries
- Signal handling
- Lifecycle hooks

### Control Plane

Built-in web dashboard for monitoring jobs, queues, and workers in real-time.

![Control Plane](docs/images/control-plane.png)

## Database Support

- SQLite
- PostgreSQL
- MySQL

## Quick Start

### Module Runner (Recommended)

Define jobs in a package:

```python
# jobs/email_job.py
import quebec

class EmailJob(quebec.BaseClass):
    queue_as = "default"

    def perform(self, to, subject):
        self.logger.info(f"Sending email to {to}: {subject}")
```

Export them in `__init__.py`:

```python
# jobs/__init__.py
from .email_job import EmailJob
```

Run with `python -m quebec`:

```bash
DATABASE_URL=sqlite:///demo.db?mode=rwc python -m quebec jobs
```

All configuration via `QUEBEC_*` environment variables — no boilerplate entry script needed.

### Script Mode

For more control, use Quebec directly in a script:

```python
import logging
from pathlib import Path
from quebec.logger import setup_logging

setup_logging(level=logging.DEBUG)

import quebec

db_path = Path('demo.db')
qc = quebec.Quebec(f'sqlite://{db_path}?mode=rwc')


@qc.register_job
class FakeJob(quebec.BaseClass):
    def perform(self, *args, **kwargs):
        self.logger.info(f"Processing job {self.id}: args={args}, kwargs={kwargs}")


if __name__ == "__main__":
    # Enqueue a job
    FakeJob.perform_later(qc, 123, foo='bar')

    # Start Quebec (handles signal, spawns workers, runs main loop)
    qc.run(
        create_tables=not db_path.exists(),
        control_plane='127.0.0.1:5006',  # Optional: web dashboard
    )
```

Or run the quickstart script directly:

```bash
curl -O https://raw.githubusercontent.com/ratazzi/quebec/refs/heads/master/quickstart.py
uv run quickstart.py
```

### Auto-Discovering Jobs

If your jobs are organized in a package (e.g. `app.jobs.*`), call
`Quebec.discover_jobs()` instead of decorating each class with
`@qc.register_job` or calling `qc.register_job_class(...)` one by one:

```python
# app/jobs/cleanup.py
class CleanupJob(quebec.BaseClass):
    def perform(self, *args, **kwargs): ...

# main.py
qc = quebec.Quebec(dsn)
qc.discover_jobs("app.jobs", "worker.tasks")   # recursively scans each
qc.run()
```

`discover_jobs` takes one or more dotted package paths as positional
arguments (varargs) — no need to wrap a single package in a list.

`discover_jobs(*packages, recursive=True, on_error="raise")`:

- Registers every `BaseClass` subclass whose `__module__` falls under one of
  the given packages. Classes imported from elsewhere (e.g. `from
  some.lib import JobMixin`) are ignored.
- Raises `ValueError` if two discovered classes share the same
  `__qualname__`, since Quebec's worker registry is keyed by qualname and
  the later registration would otherwise silently replace the earlier one.
- `on_error="raise"` (default) propagates submodule `ImportError`. Pass
  `on_error="warn"` to emit a `RuntimeWarning` and keep scanning —
  useful when a package contains optional-integration modules that may
  fail to import in some environments. The top-level package is always
  imported strictly.

### `qc.run()` Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `create_tables` | `bool` | `False` | Create database tables (requires DDL permissions) |
| `control_plane` | `str` | `None` | Web dashboard address, e.g. `'127.0.0.1:5006'` |
| `spawn` | `list[str]` | `None` | Components to spawn: `['worker', 'dispatcher', 'scheduler']`. `None` = all |

Recommended: configure worker thread count in `queue.yml` via `workers.threads`.
If you need a one-off override, `Quebec(..., worker_threads=3)` is also supported.

### Delayed Jobs

```python
from datetime import timedelta

# Run after 1 hour
FakeJob.set(wait=3600).perform_later(qc, arg1)

# Run at specific time
FakeJob.set(wait_until=tomorrow_9am).perform_later(qc, arg1)

# Override queue and priority
FakeJob.set(queue='critical', priority=1).perform_later(qc, arg1)
```

### Automatic Retries

```python
from datetime import timedelta

class PaymentJob(quebec.BaseClass):
    retry_on = [
        quebec.RetryStrategy(
            (ConnectionError, TimeoutError),
            wait=timedelta(seconds=30),
            attempts=3,
        ),
        quebec.RetryStrategy(
            (ValueError,),
            wait=timedelta(seconds=5),
            attempts=1,
            handler=lambda exc: True,  # optional callback
        ),
    ]

    def perform(self, order_id):
        process_payment(order_id)
```

Multiple `RetryStrategy` entries can target different exception types with independent wait/attempts.

### Concurrency Control

Limit how many jobs with the same key can run simultaneously:

```python
class ReportJob(quebec.BaseClass):
    concurrency_limit = 3          # max 3 concurrent executions per key
    concurrency_duration = 120     # semaphore TTL in seconds

    def concurrency_key(self, account_id, **kwargs):
        return str(account_id)     # final key: "ReportJob/123"

    def perform(self, account_id):
        generate_report(account_id)
```

The actual concurrency key is `"ClassName/key"` (e.g. `"ReportJob/123"`), so different job classes never conflict. When the limit is reached, new jobs are blocked until a slot becomes available. The `concurrency_duration` acts as a safety TTL — the semaphore is released automatically if a worker crashes.

## Lifecycle Hooks

Quebec provides several lifecycle hooks that you can use to execute code at different stages of the application lifecycle:

- `@qc.on_start`: Called when Quebec starts
- `@qc.on_stop`: Called when Quebec stops
- `@qc.on_worker_start`: Called when a worker starts
- `@qc.on_worker_stop`: Called when a worker stops
- `@qc.on_shutdown`: Called during graceful shutdown

These hooks are useful for:
- Initializing resources
- Cleaning up resources
- Logging application state
- Monitoring worker lifecycle
- Graceful shutdown handling
