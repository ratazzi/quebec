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
    AllocatingJob.perform_later(qc, 40)
    execution = _run_one(qc)
    metric = execution.metric

    if LINUX:
        # 40 MiB is above glibc's 32 MiB dynamic mmap threshold, so it is a fresh
        # mapping whatever ran before; touched page by page it takes at least
        # 40 MiB / 64 KiB faults even with the largest fault-around granularity.
        assert metric.minflt >= 640
        assert metric.new_rss_bytes >= 40 << 20
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

    AllocatingJob.perform_later(qc, 40)
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
        assert int(first["minflt"]) >= 640
        assert int(first["new_rss_kb"]) >= 40 << 10
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
    assert set(failing) == {"count", "failed", "duration_ms", "new_rss_kb", "minflt_sum"}
    assert set(failing["new_rss_kb"]) == {"avg", "p50", "p95", "max", "max_jid"}
    rss = alloc["new_rss_kb"]
    if LINUX:
        assert rss["max"] >= 40 << 10
        # p95 of two samples is the bucket holding the 40 MiB job; percentiles
        # are bucket upper bounds. The 1 MiB job may be served from reused heap
        # pages (0 faults), so p50 is only known to be >= 0.
        assert rss["p95"] >= rss["max"] >= rss["p50"] >= 0
        assert len(rss["max_jid"]) > 0
        assert alloc["minflt_sum"] >= 640
    else:
        assert rss == {"avg": 0, "p50": 0, "p95": 0, "max": 0, "max_jid": ""}

    qc.log_job_metrics_summary()
    assert qc.job_metrics_summary(reset=True)["AllocatingJob"]["count"] == 2
    assert qc.job_metrics_summary() == {}
