"""Constructor-time validation of queue.yml dispatcher intervals.

An out-of-range interval must surface as a readable ``ValueError`` from the
Quebec constructor, not a Rust panic (`Duration::from_secs_f64` panics on
negative / non-finite / overflowing input). Mirrors the worker-path validation.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

import quebec


@pytest.fixture(autouse=True)
def _clear_interval_overrides(monkeypatch) -> None:
    for key in (
        "QUEBEC_WORKER_POLLING_INTERVAL",
        "QUEBEC_DISPATCHER_POLLING_INTERVAL",
        "QUEBEC_DISPATCHER_BATCH_SIZE",
        "QUEBEC_DISPATCHER_CONCURRENCY_MAINTENANCE_INTERVAL",
    ):
        monkeypatch.delenv(key, raising=False)


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


def test_dispatcher_polling_interval_overflow_raises(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    """A huge finite value overflows Duration and must also raise, not panic."""
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - polling_interval: 1.0e30
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    with pytest.raises(ValueError, match="dispatcher_polling_interval must be finite"):
        quebec.Quebec(db_url, table_name_prefix=test_prefix)


def test_worker_polling_interval_overflow_raises(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  workers:
    - polling_interval: 1.0e30
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    with pytest.raises(ValueError, match="worker_polling_interval must be finite"):
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


def test_dispatcher_explicit_default_not_overridden_by_yaml(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    """An explicit code value equal to the default must not be overridden by
    a different queue.yml value (the value-equality check couldn't tell an
    explicit setting from an untouched default)."""
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - polling_interval: 5.0
      batch_size: 100
      concurrency_maintenance_interval: 30.0
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    # 1 second / 500 / 600 seconds are the defaults, passed explicitly using
    # supported constructor types. queue.yml sets different values for all three.
    inst = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        dispatcher_polling_interval=timedelta(seconds=1),
        dispatcher_batch_size=500,
        dispatcher_concurrency_maintenance_interval=timedelta(seconds=600),
    )
    try:
        cfg = inst._config_snapshot()
        assert cfg["dispatcher_polling_interval"] == 1.0
        assert cfg["dispatcher_batch_size"] == 500
        assert cfg["dispatcher_concurrency_maintenance_interval"] == 600.0
    finally:
        inst.close()


def test_dispatcher_yaml_applies_when_not_explicit(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    """With no code/env override, the queue.yml value takes effect."""
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - polling_interval: 5.0
      batch_size: 100
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        cfg = inst._config_snapshot()
        assert cfg["dispatcher_polling_interval"] == 5.0
        assert cfg["dispatcher_batch_size"] == 100
    finally:
        inst.close()
