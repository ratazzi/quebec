"""Per-job memory metrics: Metric fields and the CSV recorder."""

from __future__ import annotations

import csv
import sys
import time

import pytest
import quebec

LINUX = sys.platform.startswith("linux")


class AllocatingJob(quebec.BaseClass):
    def perform(self, mb: int, hold_ms: int = 0) -> None:
        buf = bytearray(mb << 20)
        for i in range(0, len(buf), 4096):
            buf[i] = 1
        if hold_ms:
            time.sleep(hold_ms / 1000)


class FailingJob(quebec.BaseClass):
    def perform(self) -> None:
        raise RuntimeError("boom")


def _run_one(qc):
    execution = qc.drain_one()
    try:
        execution.perform()
    except Exception:
        pass
    return execution


def test_metric_exposes_faults_and_process_rss_context(qc) -> None:
    qc.register_job(AllocatingJob)
    AllocatingJob.perform_later(qc, 40)
    execution = _run_one(qc)
    metric = execution.metric

    if LINUX:
        assert metric.minor_faults > 0
        assert metric.major_faults >= 0
        assert metric.process_rss_peak_bytes >= metric.process_rss_start_bytes > 0
        assert metric.process_rss_peak_bytes >= metric.process_rss_end_bytes > 0
        assert metric.process_rss_peak_delta_bytes == (
            metric.process_rss_peak_bytes - metric.process_rss_start_bytes
        )
        # This fixture is a standalone process, not a dedicated supervisor child.
        assert metric.process_rss_single_job is False
    else:
        assert metric.minor_faults is None
        assert metric.major_faults is None
        assert metric.process_rss_start_bytes is None
        assert metric.process_rss_peak_bytes is None
        assert metric.process_rss_end_bytes is None
        assert metric.process_rss_peak_delta_bytes is None
        assert metric.process_rss_single_job is False


@pytest.mark.skipif(not LINUX, reason="Linux RSS sampling")
def test_supervised_single_thread_worker_attributes_rss(
    sqlite_url, test_prefix
) -> None:
    qc = quebec.Quebec(
        sqlite_url,
        table_name_prefix=test_prefix,
        worker_threads=1,
    )
    try:
        assert qc.create_tables() is True
        qc.watch_parent_pid()  # mark this process as a supervisor worker in the test
        qc.register_job(AllocatingJob)
        qc.job_metrics_summary(reset=True)

        AllocatingJob.perform_later(qc, 40, 250)
        metric = _run_one(qc).metric

        assert metric.process_rss_single_job is True
        assert metric.process_rss_peak_delta_bytes >= 32 << 20
        memory = qc.job_metrics_summary()["AllocatingJob"][
            "process_rss_peak_delta_kb"
        ]
        assert memory["samples"] == 1
        assert memory["max"] >= 32 << 10
    finally:
        qc.close()


def test_csv_recorder_writes_one_row_per_job(qc, tmp_path) -> None:
    qc.register_job(AllocatingJob)
    qc.register_job(FailingJob)
    path = tmp_path / "metrics.csv"

    assert qc.job_metrics_path is None
    assert qc.start_job_metrics(str(path)) == str(path)
    assert qc.job_metrics_path == str(path)
    with pytest.raises(RuntimeError):
        qc.start_job_metrics(str(tmp_path / "other.csv"))

    AllocatingJob.perform_later(qc, 40, 150)
    FailingJob.perform_later(qc)
    _run_one(qc)
    _run_one(qc)

    summary = qc.stop_job_metrics()
    assert summary == {"path": str(path), "rows": 2, "dropped": 0}
    assert qc.job_metrics_path is None
    assert qc.stop_job_metrics() is None

    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["class"] for r in rows] == ["AllocatingJob", "FailingJob"]
    assert [r["status"] for r in rows] == ["executed", "failed"]
    assert list(rows[0]) == [
        "ts_ms",
        "pid",
        "tid",
        "jid",
        "class",
        "queue",
        "status",
        "duration_ms",
        "minor_faults",
        "major_faults",
        "process_rss_start_kb",
        "process_rss_peak_kb",
        "process_rss_end_kb",
        "process_rss_peak_delta_kb",
        "process_rss_single_job",
        "active_jobs",
    ]
    first = rows[0]
    assert first["queue"] == "default"
    assert len(first["jid"]) > 0
    assert float(first["duration_ms"]) >= 0
    assert first["process_rss_single_job"] == "false"
    if LINUX:
        assert int(first["minor_faults"]) > 0
        assert int(first["process_rss_start_kb"]) > 0
        assert int(first["process_rss_peak_kb"]) >= int(
            first["process_rss_start_kb"]
        )
        assert int(first["process_rss_end_kb"]) > 0
        assert int(first["tid"]) > 0
    else:
        assert first["minor_faults"] == ""
        assert first["process_rss_start_kb"] == ""


def test_toggle_starts_then_stops(qc, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("QUEBEC_JOB_METRICS_DIR", str(tmp_path))
    assert qc.toggle_job_metrics() is None
    path = qc.job_metrics_path
    assert path is not None and path.startswith(str(tmp_path))

    summary = qc.toggle_job_metrics()
    assert summary["path"] == path
    assert summary["rows"] == 0
    assert qc.job_metrics_path is None
    # Header row is written even with no jobs.
    with open(path, newline="") as f:
        assert next(csv.reader(f))[:2] == ["ts_ms", "pid"]


def test_recorder_stops_at_row_limit(qc, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("QUEBEC_JOB_METRICS_MAX_ROWS", "1")
    qc.register_job(AllocatingJob)
    path = tmp_path / "metrics.csv"
    qc.start_job_metrics(str(path))

    AllocatingJob.perform_later(qc, 1)
    AllocatingJob.perform_later(qc, 1)
    _run_one(qc)
    assert qc.job_metrics_path is None  # limit reached after the first row
    _run_one(qc)
    assert qc.stop_job_metrics() is None

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with path.open(newline="") as f:
            if len(list(csv.DictReader(f))) == 1:
                break
        time.sleep(0.01)
    else:
        pytest.fail("background metrics writer did not flush the row")


def test_per_class_summary_aggregates_in_process(qc) -> None:
    qc.register_job(AllocatingJob)
    qc.register_job(FailingJob)
    qc.job_metrics_summary(reset=True)  # other tests in this process

    AllocatingJob.perform_later(qc, 40)
    AllocatingJob.perform_later(qc, 1)
    FailingJob.perform_later(qc)
    for _ in range(3):
        _run_one(qc)

    summary = qc.job_metrics_summary()
    assert set(summary) == {"AllocatingJob", "FailingJob"}
    alloc = summary["AllocatingJob"]
    assert alloc["count"] == 2 and alloc["failed"] == 0
    assert alloc["duration_ms"]["max"] >= alloc["duration_ms"]["avg"] > 0
    failing = summary["FailingJob"]
    assert failing["count"] == 1 and failing["failed"] == 1
    assert set(failing) == {
        "count",
        "failed",
        "duration_ms",
        "minor_faults",
        "process_rss_peak_delta_kb",
    }
    assert set(failing["minor_faults"]) == {"samples", "sum"}
    assert set(failing["process_rss_peak_delta_kb"]) == {
        "samples",
        "avg",
        "p50",
        "p95",
        "max",
        "max_jid",
    }
    rss = alloc["process_rss_peak_delta_kb"]
    if LINUX:
        assert alloc["minor_faults"]["samples"] == 2
        assert alloc["minor_faults"]["sum"] > 0
    else:
        assert alloc["minor_faults"] == {"samples": 0, "sum": 0}

    # Standalone workers deliberately do not claim process RSS attribution.
    assert rss == {
        "samples": 0,
        "avg": None,
        "p50": None,
        "p95": None,
        "max": None,
        "max_jid": None,
    }

    qc.log_job_metrics_summary()
    assert qc.job_metrics_summary(reset=True)["AllocatingJob"]["count"] == 2
    assert qc.job_metrics_summary() == {}


def test_metrics_state_is_per_quebec_instance(sqlite_url, tmp_path) -> None:
    qc1 = quebec.Quebec(sqlite_url, table_name_prefix="metrics_one")
    qc2 = quebec.Quebec(sqlite_url, table_name_prefix="metrics_two")
    try:
        assert qc1.create_tables() is True
        assert qc2.create_tables() is True
        qc1.register_job(AllocatingJob)
        qc2.register_job(FailingJob)

        assert qc1.start_job_metrics(str(tmp_path / "one.csv"))
        assert qc2.start_job_metrics(str(tmp_path / "two.csv"))

        AllocatingJob.perform_later(qc1, 1)
        FailingJob.perform_later(qc2)
        _run_one(qc1)
        _run_one(qc2)

        assert set(qc1.job_metrics_summary()) == {"AllocatingJob"}
        assert set(qc2.job_metrics_summary()) == {"FailingJob"}
        assert qc1.stop_job_metrics()["rows"] == 1
        assert qc2.stop_job_metrics()["rows"] == 1
    finally:
        qc1.close()
        qc2.close()
