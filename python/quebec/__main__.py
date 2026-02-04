"""
Quebec CLI runner — auto-discover and run job classes.

Usage:
    python -m quebec app.jobs
    python -m quebec app.jobs app.other_jobs

All configuration via environment variables:

    QUEBEC_DATABASE_URL or DATABASE_URL  — database connection string (required)
    QUEBEC_THREADS          — Python worker threads (default: 1)
    QUEBEC_CREATE_TABLES    — create tables on startup (default: true, set false/0/no to disable)
    QUEBEC_CONTROL_PLANE    — control plane address, e.g. "127.0.0.1:5006"
    QUEBEC_SPAWN            — comma-separated components: worker,dispatcher,scheduler
    QUEBEC_LOG_LEVEL        — log level (default: INFO)
    QUEBEC_LOG_FORMAT       — structlog format: console, json, logfmt (default: console)

    Other QUEBEC_* env vars (pool, polling, etc.) are handled by the Rust core.
"""

import importlib
import inspect
import logging
import os
import sys

from quebec import BaseClass, Quebec
from quebec.logger import setup_structlog, get_structlog


def discover_job_classes(module):
    """Find all BaseClass subclasses defined in a module."""
    return [
        obj
        for _name, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, BaseClass) and obj is not BaseClass
    ]


def parse_bool(value):
    return value.lower() in ("true", "1", "yes")


def main():
    modules = sys.argv[1:]
    if not modules:
        print(__doc__.strip(), file=sys.stderr)
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

    # Discover and register job classes
    total = 0
    for module_path in modules:
        try:
            mod = importlib.import_module(module_path)
        except ImportError as e:
            logger.error(f"Cannot import module '{module_path}': {e}")
            sys.exit(1)

        classes = discover_job_classes(mod)
        if not classes:
            logger.warning(f"No BaseClass subclasses found in '{module_path}'")
            continue

        for cls in classes:
            qc.register_job(cls)
            logger.info(f"Registered {cls.__name__} from {module_path}")
            total += 1

    if total == 0:
        logger.error("No job classes found in any of the specified modules")
        sys.exit(1)

    logger.info(f"Discovered {total} job class(es), starting Quebec")

    # Run options
    threads = int(os.environ.get("QUEBEC_THREADS", "1"))
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
        threads=threads,
    )


if __name__ == "__main__":
    main()
