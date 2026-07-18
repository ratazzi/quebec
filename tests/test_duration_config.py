"""Duration constructor options share one safe Python conversion path."""

from __future__ import annotations

import math

import pytest

import quebec


FLOAT_DURATION_OPTIONS = {
    "notify_throttle_interval": 0.125,
    "process_heartbeat_interval": 1.25,
    "process_alive_threshold": 2.25,
    "shutdown_timeout": 3.25,
    "clear_finished_jobs_after": 4.25,
    "cleanup_interval": 5.25,
    "default_concurrency_control_period": 6.25,
    "dispatcher_polling_interval": 7.25,
    "dispatcher_concurrency_maintenance_interval": 8.25,
    "control_plane_sse_interval": 9.25,
    "worker_polling_interval": 0.05,
    "worker_memory_check_interval": 10.25,
    "worker_memory_graceful_timeout": 11.25,
}


def test_all_duration_kwargs_accept_float_seconds(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv("QUEBEC_CONFIG", str(tmp_path / "missing.yml"))

    inst = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        **FLOAT_DURATION_OPTIONS,
    )
    try:
        snapshot = inst._config_snapshot()
        assert {key: snapshot[key] for key in FLOAT_DURATION_OPTIONS} == pytest.approx(
            FLOAT_DURATION_OPTIONS
        )
    finally:
        inst.close()


@pytest.mark.parametrize("invalid", [-1.0, math.nan, math.inf, 1.0e300])
def test_invalid_float_duration_falls_back_without_panicking(
    invalid, tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv("QUEBEC_CONFIG", str(tmp_path / "missing.yml"))
    monkeypatch.delenv("QUEBEC_SHUTDOWN_TIMEOUT", raising=False)

    inst = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        shutdown_timeout=invalid,
    )
    try:
        assert inst._config_snapshot()["shutdown_timeout"] == 5.0
    finally:
        inst.close()


def test_invalid_worker_polling_kwarg_does_not_fall_back_to_env(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv("QUEBEC_CONFIG", str(tmp_path / "missing.yml"))
    monkeypatch.setenv("QUEBEC_WORKER_POLLING_INTERVAL", "30")

    inst = quebec.Quebec(
        db_url,
        table_name_prefix=test_prefix,
        worker_polling_interval=math.nan,
    )
    try:
        assert inst._config_snapshot()["worker_polling_interval"] == 0.1
    finally:
        inst.close()
