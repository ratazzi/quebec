"""Per-job memory metrics: Metric fields and the CSV recorder."""

from __future__ import annotations

import csv
import sys

import pytest
import quebec

LINUX = sys.platform.startswith("linux")


class AllocatingJob(quebec.BaseClass):
    def perform(self, mb: int) -> None:
        buf = bytearray(mb << 20)
        for i in range(0, len(buf), 4096):
            buf[i] = 1


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


def test_metric_exposes_page_faults(qc) -> None:
    qc.register_job(AllocatingJob)
    AllocatingJob.perform_later(qc, 8)
    execution = _run_one(qc)
    metric = execution.metric

    if LINUX:
        # 8 MiB touched page by page: at least 8 MiB / 64 KiB faults even with
        # the largest common fault-around granularity.
        assert metric.minflt >= 128
        assert metric.new_rss_bytes >= 8 << 20
    else:
        assert metric.minflt is None
        assert metric.new_rss_bytes is None


def test_csv_recorder_writes_one_row_per_job(qc, tmp_path) -> None:
    qc.register_job(AllocatingJob)
    qc.register_job(FailingJob)
    path = tmp_path / "metrics.csv"

    assert qc.job_metrics_path is None
    assert qc.start_job_metrics(str(path)) == str(path)
    assert qc.job_metrics_path == str(path)
    with pytest.raises(RuntimeError):
        qc.start_job_metrics(str(tmp_path / "other.csv"))

    AllocatingJob.perform_later(qc, 8)
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
        "minflt",
        "majflt",
        "new_rss_kb",
        "proc_rss_kb",
        "active_jobs",
    ]
    first = rows[0]
    assert first["queue"] == "default"
    assert len(first["jid"]) > 0
    assert float(first["duration_ms"]) >= 0
    assert int(first["proc_rss_kb"]) > 0
    if LINUX:
        assert int(first["minflt"]) >= 128
        assert int(first["new_rss_kb"]) >= 8 << 10
        assert int(first["tid"]) > 0
    else:
        assert first["minflt"] == ""
        assert first["new_rss_kb"] == ""


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

    with path.open(newline="") as f:
        assert len(list(csv.DictReader(f))) == 1
