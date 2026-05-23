"""Tests for the experimental sliding-window rate limit feature.

Spec: docs/superpowers/specs/2026-05-23-experimental-rate-limit-sliding-window-design.md
"""

from datetime import timedelta

import pytest

import quebec


def test_rate_limit_conflict_enum_exposed():
    assert hasattr(quebec, "RateLimitConflict")
    assert quebec.RateLimitConflict.Reschedule != quebec.RateLimitConflict.Discard
    assert repr(quebec.RateLimitConflict.Reschedule) == "RateLimitConflict.Reschedule"
    assert repr(quebec.RateLimitConflict.Discard) == "RateLimitConflict.Discard"


def test_register_job_extracts_rate_limit_config(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class ApiCall(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_window = timedelta(seconds=2)
        rate_limit_on_throttle = quebec.RateLimitConflict.Discard

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(ApiCall)

    cfg = qc._rate_limit_config_for(ApiCall.__qualname__)
    assert cfg is not None
    assert cfg["max"] == 5
    assert cfg["window_seconds"] == 2
    assert cfg["on_throttle"] == quebec.RateLimitConflict.Discard


def test_register_job_without_rate_limit_returns_none(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class PlainJob(quebec.BaseClass):
        def perform(self, *args, **kwargs):
            pass

    qc.register_job(PlainJob)
    assert qc._rate_limit_config_for(PlainJob.__qualname__) is None


def test_register_job_defaults_to_reschedule(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class DefaultThrottle(quebec.BaseClass):
        rate_limit_max = 10
        rate_limit_window = timedelta(seconds=1)

        def perform(self, *args, **kwargs):
            pass

    qc.register_job(DefaultThrottle)
    cfg = qc._rate_limit_config_for(DefaultThrottle.__qualname__)
    assert cfg["on_throttle"] == quebec.RateLimitConflict.Reschedule


def test_register_job_rejects_subsecond_window(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class TooFast(quebec.BaseClass):
        rate_limit_max = 5
        rate_limit_window = timedelta(milliseconds=500)

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_window must be >= 1 second"):
        qc.register_job(TooFast)


def test_register_job_rejects_max_without_window(qc_with_sqlalchemy):
    qc = qc_with_sqlalchemy["qc"]

    class MissingWindow(quebec.BaseClass):
        rate_limit_max = 5

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_window"):
        qc.register_job(MissingWindow)
