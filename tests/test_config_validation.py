"""Constructor-time validation of queue.yml dispatcher intervals.

An out-of-range interval must surface as a readable ``ValueError`` from the
Quebec constructor, not a Rust panic (`Duration::from_secs_f64` panics on
negative / non-finite input). Mirrors the worker-path validation.
"""

from __future__ import annotations

import pytest

import quebec


def _queue_yml(tmp_path, body: str) -> str:
    path = tmp_path / "queue.yml"
    path.write_text(body)
    return str(path)


def test_dispatcher_polling_interval_negative_raises(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - polling_interval: -1
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    with pytest.raises(ValueError, match="dispatcher_polling_interval must be finite"):
        quebec.Quebec(db_url, table_name_prefix=test_prefix)


def test_dispatcher_concurrency_maintenance_interval_negative_raises(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - concurrency_maintenance_interval: -5
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    with pytest.raises(
        ValueError, match="concurrency_maintenance_interval must be finite"
    ):
        quebec.Quebec(db_url, table_name_prefix=test_prefix)


def test_dispatcher_polling_interval_valid_ok(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    """A valid interval still constructs (guards against over-strict checks)."""
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - polling_interval: 2.5
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    inst.close()
