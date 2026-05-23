"""Tests for the experimental sliding-window rate limit feature.

Spec: docs/superpowers/specs/2026-05-23-experimental-rate-limit-sliding-window-design.md
"""

import quebec


def test_rate_limit_conflict_enum_exposed():
    assert hasattr(quebec, "RateLimitConflict")
    assert quebec.RateLimitConflict.Reschedule != quebec.RateLimitConflict.Discard
    assert repr(quebec.RateLimitConflict.Reschedule) == "RateLimitConflict.Reschedule"
    assert repr(quebec.RateLimitConflict.Discard) == "RateLimitConflict.Discard"
