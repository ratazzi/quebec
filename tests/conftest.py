"""Pytest configuration and shared fixtures for Quebec integration tests."""

from __future__ import annotations

import os
import tempfile
import uuid

import maturin_import_hook
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

maturin_import_hook.install()

import quebec  # noqa: E402


def generate_test_prefix():
    """Generate a unique table prefix for test isolation."""
    # Use short UUID to keep table names reasonable
    short_id = uuid.uuid4().hex[:8]
    return f"t_{short_id}"


@pytest.fixture
def test_prefix():
    """Generate a unique table prefix for this test."""
    return generate_test_prefix()


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    try:
        os.unlink(path)
    except OSError:
        pass


# Database URL fixtures
@pytest.fixture
def sqlite_url(temp_db_path):
    """SQLite database URL (file-based, SQLAlchemy 4-slash absolute form)."""
    return f"sqlite:///{temp_db_path}?mode=rwc"


@pytest.fixture
def sqlite_memory_url():
    """SQLite in-memory database URL."""
    return "sqlite::memory:"


@pytest.fixture(params=["sqlite"])
def db_url(request, temp_db_path):
    """
    Parametrized database URL fixture.

    To test with multiple databases, add to params:
    params=['sqlite', 'postgresql', 'mysql']

    And set environment variables:
    - TEST_POSTGRESQL_URL=postgresql://user:pass@localhost/test_db
    - TEST_MYSQL_URL=mysql://user:pass@localhost/test_db
    """
    if request.param == "sqlite":
        return f"sqlite:///{temp_db_path}?mode=rwc"
    elif request.param == "postgresql":
        url = os.environ.get("TEST_POSTGRESQL_URL")
        if not url:
            pytest.skip("TEST_POSTGRESQL_URL not set")
        return url
    elif request.param == "mysql":
        url = os.environ.get("TEST_MYSQL_URL")
        if not url:
            pytest.skip("TEST_MYSQL_URL not set")
        return url


@pytest.fixture
def qc(db_url, test_prefix):
    """
    Quebec instance with unique table prefix for test isolation.

    Each test gets its own set of tables, allowing parallel execution.
    """
    instance = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    assert instance.create_tables() is True

    yield instance

    instance.close()


@pytest.fixture
def qc_with_sqlalchemy(db_url, test_prefix, temp_db_path):
    """
    Quebec instance with SQLAlchemy session for verification.

    Returns a dict with:
    - qc: Quebec instance
    - session: SQLAlchemy session for querying
    - prefix: table prefix used
    """
    instance = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    assert instance.create_tables() is True

    # Create SQLAlchemy engine for test verification.
    # db_url already uses SQLAlchemy's 4-slash absolute form; just drop
    # any sqlx-only query params (e.g. ?mode=rwc) that SA doesn't parse.
    sa_url = db_url.split("?")[0] if db_url.startswith("sqlite:") else db_url

    engine = create_engine(sa_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    yield {
        "qc": instance,
        "engine": engine,
        "session": session,
        "prefix": test_prefix,
        "db_url": db_url,
    }

    instance.close()
    session.close()
    engine.dispose()


@pytest.fixture
def fake_job_class():
    """Create a simple FakeJob class for testing."""

    class FakeJob(quebec.ActiveJob):
        performed_args = []

        def perform(self, *args, **kwargs):
            FakeJob.performed_args.append((args, kwargs))
            return True

    # Reset state for each test
    FakeJob.performed_args = []
    return FakeJob


@pytest.fixture
def registered_qc(qc, fake_job_class):
    """Quebec instance with FakeJob registered."""
    qc.register_job(fake_job_class)
    return qc


# Helper functions for integration tests
class DBAssert:
    """Helper class for database assertions in integration tests."""

    def __init__(self, session, prefix):
        self.session = session
        self.prefix = prefix

    def _table(self, name):
        """Get full table name with prefix."""
        return f"{self.prefix}_{name}"

    def count_jobs(self):
        """Count total jobs."""
        result = self.session.execute(
            text(f"SELECT COUNT(*) FROM {self._table('jobs')}")
        )
        return result.scalar()

    def count_ready_executions(self):
        """Count ready executions."""
        result = self.session.execute(
            text(f"SELECT COUNT(*) FROM {self._table('ready_executions')}")
        )
        return result.scalar()

    def count_claimed_executions(self):
        """Count claimed executions."""
        result = self.session.execute(
            text(f"SELECT COUNT(*) FROM {self._table('claimed_executions')}")
        )
        return result.scalar()

    def count_failed_executions(self):
        """Count failed executions."""
        result = self.session.execute(
            text(f"SELECT COUNT(*) FROM {self._table('failed_executions')}")
        )
        return result.scalar()

    def count_blocked_executions(self):
        """Count blocked executions."""
        result = self.session.execute(
            text(f"SELECT COUNT(*) FROM {self._table('blocked_executions')}")
        )
        return result.scalar()

    def count_scheduled_executions(self):
        """Count scheduled executions."""
        result = self.session.execute(
            text(f"SELECT COUNT(*) FROM {self._table('scheduled_executions')}")
        )
        return result.scalar()

    def get_job(self, job_id):
        """Get job by ID."""
        result = self.session.execute(
            text(f"SELECT * FROM {self._table('jobs')} WHERE id = :id"), {"id": job_id}
        )
        row = result.fetchone()
        if row:
            return dict(row._mapping)
        return None

    def get_ready_execution(self, job_id):
        """Get ready execution by job_id."""
        result = self.session.execute(
            text(
                f"SELECT * FROM {self._table('ready_executions')} WHERE job_id = :job_id"
            ),
            {"job_id": job_id},
        )
        row = result.fetchone()
        if row:
            return dict(row._mapping)
        return None

    def job_is_finished(self, job_id):
        """Check if job has finished_at set."""
        job = self.get_job(job_id)
        return job and job.get("finished_at") is not None

    def job_is_failed(self, job_id):
        """Check if job has a failed execution record."""
        result = self.session.execute(
            text(
                f"SELECT COUNT(*) FROM {self._table('failed_executions')} WHERE job_id = :job_id"
            ),
            {"job_id": job_id},
        )
        return result.scalar() > 0


@pytest.fixture
def db_assert(qc_with_sqlalchemy):
    """Database assertion helper."""
    return DBAssert(qc_with_sqlalchemy["session"], qc_with_sqlalchemy["prefix"])
