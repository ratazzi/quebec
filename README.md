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
- Per-queue concurrency limits
- Rate limiting
- Exclusive (stop-the-world) jobs
- Multi-process (fork) mode
- Memory-based worker recycling
- Web dashboard
- Automatic retries
- Signal handling & graceful restart
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
    # Enqueue a job (qc is inferred from @qc.register_job)
    FakeJob.perform_later(123, foo='bar')

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

### Multiple Quebec Instances

Quebec is designed for one instance per process. Registering a job class
(via `@qc.register_job`, `qc.register_job_class`, or `qc.discover_jobs`)
binds it to that Quebec instance, so `MyJob.perform_later(...)` shorthand
routes to the binding. If a process holds more than one Quebec instance
and registers the same job class to each, the most recent registration
wins — pass the target instance explicitly to disambiguate:

```python
MyJob.perform_later(qc2, arg1)                  # route to qc2
MyJob.set(queue='critical').perform_later(qc2, arg1)
```

### `qc.run()` Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `create_tables` | `bool` | `False` | Create database tables (requires DDL permissions) |
| `control_plane` | `str` | `None` | Web dashboard address, e.g. `'127.0.0.1:5006'` |
| `spawn` | `list[str]` | `None` | Components to spawn: `['worker', 'dispatcher', 'scheduler']`. `None` = all |

Recommended: configure worker thread count in `queue.yml` via `workers.threads`.
If you need a one-off override, `Quebec(..., worker_threads=3)` is also supported.

### Multi-Process Mode (fork supervisor)

By default `qc.run()` runs all components as threads in a single process. To scale across CPU cores, set `QUEBEC_SUPERVISOR=1` to fork a pool of child processes instead:

```bash
QUEBEC_SUPERVISOR=1 python -m quebec your.jobs
```

```yaml
# queue.yml (under your environment, e.g. production:)
workers:
  - queues: "*"
    threads: 5
    processes: 4        # fork 4 worker processes
dispatchers:
  - polling_interval: 1
    processes: 1        # fork 1 dispatcher process
```

The supervisor forks `workers[].processes` worker children and `dispatchers[].processes` dispatcher children, each taking its config from the matching yml entry, and reforks any child that dies (matching Solid Queue's process model). Fork mode is opt-in via the env var so an existing config with `processes` set doesn't silently switch process model on upgrade; `spawn` is ignored in this mode. Outside supervisor mode the `processes` keys are ignored and Quebec uses the single-process threaded runtime.

### Force Queue Override (multi-branch development)

Set `QUEBEC_FORCE_OVERRIDE_QUEUE` to pin every enqueue and consumption to one queue — handy when several development branches share a single database:

```bash
QUEBEC_FORCE_OVERRIDE_QUEUE=branch_x python -m quebec your.jobs
```

Every enqueue path rewrites `queue_name` to this value (ignoring whatever the class, call site, or scheduler specified), and the worker only consumes that queue — so jobs enqueued by one branch are never picked up by another. URL-hostile characters in the name are sanitized to `-`.

### Transactional Enqueue

> [!IMPORTANT]
> **Enqueuing is _not_ part of your database transaction — even on the same database.** Quebec's enqueue runs through the Rust engine on its own connection pool, completely separate from your Python connection (SQLAlchemy / Django / psycopg). There is no way to atomically commit a business write and a job enqueue together.

This is the deliberate cost of keeping the engine fully decoupled from your ORM and connection — the upside is that Quebec drags no Python database dependencies into your app, but it means the enqueue cannot join your transaction. Two failure windows follow:

- The business transaction commits but the enqueue fails → **the job is lost**.
- The enqueue commits but the business transaction rolls back → **the job runs against missing or stale data**.

Recommendations:

- **Enqueue after your business transaction commits.** This removes the worse direction — a job running for a write that was rolled back.
- **Make jobs idempotent** and tolerant of data that may not be visible yet; lean on retries.
- If you genuinely need atomicity, use a **transactional outbox**: write an outbox row inside your own transaction (business + outbox commit atomically), then relay it into a real job (at-least-once delivery).

### Delayed Jobs

```python
from datetime import timedelta

# Run after 1 hour
FakeJob.set(wait=3600).perform_later(arg1)

# Run at specific time
FakeJob.set(wait_until=tomorrow_9am).perform_later(arg1)

# Override queue and priority
FakeJob.set(queue='critical', priority=1).perform_later(arg1)
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
            # Called once retries are exhausted; receives (job, error).
            handler=lambda job, error: notify_admin(error),
        ),
    ]

    def perform(self, order_id):
        process_payment(order_id)
```

Multiple `RetryStrategy` entries can target different exception types with independent wait/attempts. The optional `handler` fires only when a strategy's attempts are exhausted (mirroring ActiveJob's `retry_on ... do |job, error|` block) and is called with `(job, error)`. `discard_on` and `rescue_from` handlers use the same `(job, error)` signature.

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

### Rate Limiting (experimental)

Cap how many jobs run within a sliding time window, scoped per key:

```python
from datetime import timedelta

class ApiCallJob(quebec.BaseClass):
    rate_limit_max = 5                          # at most 5 runs...
    rate_limit_duration = timedelta(seconds=2)  # ...per rolling 2-second window
    rate_limit_on_throttle = quebec.RateLimitConflict.Reschedule  # default

    def rate_limit_key(self, region="us", **kwargs):
        return region                           # bucket key: "ApiCallJob/us"

    def perform(self, region="us"):
        call_external_api(region)
```

Like concurrency control, the bucket is `"ClassName/key"`, and `rate_limit_key` defaults to the class name when not overridden. `rate_limit_duration` must be a `datetime.timedelta` of at least one second. When the window is exhausted, `rate_limit_on_throttle` decides what happens: `Reschedule` (the default) pushes the job to a later run, while `Discard` drops it.

### Exclusive Jobs

Let an occasional memory-heavy job own the whole worker process while it runs:

```python
class RebuildSearchIndexJob(quebec.BaseClass):
    exclusive = True

    def perform(self):
        rebuild_index()                         # runs alone on this worker
```

When an `exclusive` job is claimed, the worker stops claiming new jobs, waits for any in-flight siblings to finish, then runs the exclusive job by itself before resuming normal claiming. The scope is the **current worker process** — it does not coordinate across separate worker processes; pair it with `concurrency_limit = 1` and a `concurrency_key` if you also need cluster-wide single-instance execution.

### Graceful Restart (quiet-then-exit)

Drain in-flight work and exit on a quiet signal, for zero-downtime rolling restarts:

```python
qc = quebec.Quebec(database_url="...", quiet_then_exit=True)
qc.run()
```

Sending `SIGUSR1` (or `SIGTSTP`) puts the worker into quiet mode: it stops claiming new jobs but keeps running until every in-flight job finishes, then exits cleanly — with no time limit (unlike the `SIGTERM` path, which is bounded by `shutdown_timeout`). The usual flow is: signal the old instance quiet, start a new instance, and the old one exits once drained. Opt-in (default off), and standalone-only — under the fork supervisor a self-exited child would just be reforked, so use a supervisor-level rolling restart there instead. Also settable via `QUEBEC_QUIET_THEN_EXIT=1`.

### Memory-Based Worker Recycling

Long-lived Python workers tend to hold onto RSS the interpreter never returns to the OS. Quebec can recycle a bloated worker by draining it and exiting with a dedicated code, leaving the actual restart to your process supervisor. It is configured by environment variables — there is no in-process restart:

```bash
QUEBEC_WORKER_MAX_RSS_MB=512                  # soft limit; unset = disabled
QUEBEC_WORKER_MEMORY_RECYCLE_CONFIRMATIONS=3  # consecutive over-limit samples before recycling (default)
QUEBEC_WORKER_MEMORY_CHECK_INTERVAL=5s        # how often RSS is sampled (default)
```

When a worker's RSS stays above the limit for that many consecutive samples, it enters quiet mode, stops claiming, drains its in-flight jobs (no time limit), and exits with code **75** — the planned-recycle code. The supervisor then relaunches a fresh process. Under the built-in fork supervisor (`QUEBEC_SUPERVISOR=1`) this refork is automatic; under systemd, `Restart=on-failure` relaunches the worker after the non-zero recycle exit:

```ini
# /etc/systemd/system/quebec-worker.service
[Service]
ExecStart=/usr/bin/python -m quebec your.jobs
Environment=QUEBEC_DATABASE_URL=postgresql://localhost/myapp
Environment=QUEBEC_WORKER_MAX_RSS_MB=512
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Exit code 75 is non-zero, so `Restart=on-failure` treats the planned recycle as a failure and relaunches the worker. If you'd rather not have planned recycles show up as failures (in `systemctl status` or the start-limit counter), add `SuccessExitStatus=75` together with `RestartForceExitStatus=75` — the former keeps 75 out of the failure tally, the latter still forces the restart.

### Per-Queue Concurrency (experimental)

Cap how many jobs run concurrently across the cluster for specific queues, independent of per-class `concurrency_key`:

```python
qc = quebec.Quebec(
    database_url="...",
    experimental_queue_concurrency={"reports": 2, "exports": 1},
)
qc.run()
```

Each listed queue acquires a `queue:<name>` semaphore at claim time; queues not present are unlimited. Useful for isolating a misbehaving queue during remediation. Naming and semantics are experimental and may change.

### TLS Configuration (PostgreSQL)

Quebec links `sqlx` against `rustls` + `webpki-roots`. Public CAs (AWS RDS,
Neon, Google Cloud SQL, Supabase, etc.) are trusted out of the box — no OS
trust store is consulted.

Pass libpq-style SSL options as `Quebec(...)` kwargs, as DSN query params, or
via `QUEBEC_SSL*` environment variables:

```python
qc = quebec.Quebec(
    "postgresql://user:pass@host:5432/db",
    sslmode="verify-full",             # or QUEBEC_SSLMODE
    sslrootcert="/etc/ssl/certs/ca.pem",  # internal CAs only
)
```

Priority is **kwargs > env > DSN query**. Passing any `ssl*` kwarg/env against
a non-postgres URL raises `ValueError`.

| `sslmode`     | Transport              | Certificate verification | Hostname verification |
|---------------|------------------------|--------------------------|-----------------------|
| `disable`     | plaintext              | —                        | —                     |
| `prefer`      | TLS if offered, else plaintext | —                | —                     |
| `require`     | TLS (fails if unsupported) | — (accepts any cert)  | —                     |
| `verify-ca`   | TLS                    | CA-signed                | —                     |
| `verify-full` | TLS                    | CA-signed                | hostname matches CN/SAN |

For public CAs, `verify-full` works zero-config. Use `sslrootcert` for
internal/self-signed CAs. `sslcert` + `sslkey` enable client certificate
(mTLS) auth.

> `sslmode=allow` is **rejected** with a `ValueError`. Upstream `sqlx-postgres`
> 0.8 treats `allow` identically to `disable` (plaintext, marked `FIXME` in
> the driver); to avoid a silent downgrade, Quebec refuses it. Use `prefer`
> for opportunistic TLS, or `require`/`verify-*` to enforce it.

> Note: some managed Postgres services (e.g. Neon) terminate TLS at a proxy
> layer. In those cases `pg_stat_ssl.ssl` may report `false` because the
> backend sees plaintext from the proxy — not the client.

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
