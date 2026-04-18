"""
Quebec CLI runner — auto-discover and run job classes.

Usage:
    python -m quebec app.jobs
    python -m quebec app.jobs app.other_jobs

Each positional argument is a dotted package path; the runner recursively
scans it via ``Quebec.discover_jobs`` and registers every ``BaseClass``
subclass whose ``__module__`` falls under that package.

All configuration via environment variables:

    QUEBEC_DATABASE_URL or DATABASE_URL  — database connection string (required)
    QUEBEC_WORKER_THREADS   — worker threads (default from queue.yml or 3)
    QUEBEC_CREATE_TABLES    — create tables on startup (default: true, set false/0/no to disable)
    QUEBEC_CONTROL_PLANE    — control plane address, e.g. "127.0.0.1:5006"
    QUEBEC_SPAWN            — comma-separated components: worker,dispatcher,scheduler
    QUEBEC_LOG_LEVEL        — log level (default: INFO)
    QUEBEC_LOG_FORMAT       — structlog format: console, json, logfmt (default: console)
    QUEBEC_DISCOVER_ON_ERROR — how discover_jobs handles submodule ImportError:
                              "raise" (default) or "warn"

    Other QUEBEC_* env vars (pool, polling, etc.) are handled by the Rust core.
"""

import logging
import os
import sys

from quebec import Quebec
from quebec.logger import setup_structlog, get_structlog


def main():
    modules = sys.argv[1:]
    if not modules:
        print((__doc__ or "").strip(), file=sys.stderr)
        sys.exit(1)

    # Logging
    log_level = getattr(
        logging, os.environ.get("QUEBEC_LOG_LEVEL", "INFO").upper(), logging.INFO
    )
    log_format = os.environ.get("QUEBEC_LOG_FORMAT", "console")
    setup_structlog(level=log_level, format=log_format)
    logger = get_structlog("quebec.cli")

    # Database URL
    database_url = os.environ.get("QUEBEC_DATABASE_URL") or os.environ.get(
        "DATABASE_URL"
    )
    if not database_url:
        logger.error("Set QUEBEC_DATABASE_URL or DATABASE_URL environment variable")
        sys.exit(1)

    # Create Quebec instance (QUEBEC_* env vars are read by Rust core)
    qc = Quebec(database_url)

    on_error = os.environ.get("QUEBEC_DISCOVER_ON_ERROR", "raise")
    try:
        registered = qc.discover_jobs(*modules, on_error=on_error)
    except ImportError as e:
        logger.error(f"Cannot import module: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    if not registered:
        logger.error("No job classes found in any of the specified modules")
        sys.exit(1)

    for cls in registered:
        logger.info(f"Registered {cls.__qualname__} from {cls.__module__}")

    logger.info(f"Discovered {len(registered)} job class(es), starting Quebec")

    # Run options
    create_tables_env = os.environ.get("QUEBEC_CREATE_TABLES")
    create_tables = (
        create_tables_env.lower() not in ("false", "0", "no")
        if create_tables_env
        else True
    )
    control_plane = os.environ.get("QUEBEC_CONTROL_PLANE")
    spawn_env = os.environ.get("QUEBEC_SPAWN")
    spawn = (
        [s.strip() for s in spawn_env.split(",") if s.strip()] if spawn_env else None
    ) or None

    qc.run(
        create_tables=create_tables,
        control_plane=control_plane,
        spawn=spawn,
    )


if __name__ == "__main__":
    main()
