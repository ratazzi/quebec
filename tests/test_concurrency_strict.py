"""Tests for the SQ-aligned strict concurrency_key checks.

Pre-fix behaviour silently dropped concurrency control when:
- the class set `concurrency_limit` / `concurrency_duration` but never defined
  `concurrency_key` (register-time fall-through), or
- `concurrency_key()` returned `None` / empty string (runtime fall-through).

Post-fix:
- register-time: the first case raises `ValueError`, matching SQ's
  `limits_concurrency key:` keyword being mandatory.
- runtime: None/empty falls back to a group-only key (`concurrency_group`)
  instead of NULL, mirroring SQ's `[group, key].compact.join("/")`. A warn!
  is emitted so users who forgot `return` discover the issue via logs.
"""

from __future__ import annotations

import pytest
import quebec
from sqlalchemy import text


# ──────────────────────────────────────────────────────────────────────────
# Layer 1 — register-time strict check
# ──────────────────────────────────────────────────────────────────────────


def test_register_raises_when_limit_set_without_concurrency_key(qc_with_sqlalchemy):
    """concurrency_limit set + no concurrency_key method → ValueError on register."""
    qc = qc_with_sqlalchemy["qc"]

    class MisconfiguredLimitJob(quebec.BaseClass):
        concurrency_limit = 3

        def perform(self, *args, **kwargs) -> None:
            pass

    with pytest.raises(ValueError, match="concurrency_key"):
        qc.register_job(MisconfiguredLimitJob)


def test_register_raises_when_duration_set_without_concurrency_key(qc_with_sqlalchemy):
    """concurrency_duration set + no concurrency_key method → ValueError on register."""
    qc = qc_with_sqlalchemy["qc"]

    class MisconfiguredDurationJob(quebec.BaseClass):
        concurrency_duration = 60

        def perform(self, *args, **kwargs) -> None:
            pass

    with pytest.raises(ValueError, match="concurrency_key"):
        qc.register_job(MisconfiguredDurationJob)


def test_register_accepts_class_with_no_concurrency_attrs(qc_with_sqlalchemy):
    """Class without any concurrency_* attribute is unaffected by the strict check."""
    qc = qc_with_sqlalchemy["qc"]

    class PlainJob(quebec.BaseClass):
        def perform(self, *args, **kwargs) -> None:
            pass

    qc.register_job(PlainJob)  # must not raise


def test_register_accepts_only_concurrency_on_conflict_set(qc_with_sqlalchemy):
    """concurrency_on_conflict defaults to Block, so setting it alone shouldn't trip
    the strict check — only limit/duration count as 'user configured concurrency'."""
    qc = qc_with_sqlalchemy["qc"]

    class OnConflictOnlyJob(quebec.BaseClass):
        concurrency_on_conflict = quebec.ConcurrencyConflict.discard()

        def perform(self, *args, **kwargs) -> None:
            pass

    qc.register_job(OnConflictOnlyJob)  # must not raise


def test_register_accepts_full_concurrency_config(qc_with_sqlalchemy):
    """concurrency_limit + concurrency_key method → registers cleanly."""
    qc = qc_with_sqlalchemy["qc"]

    class ProperJob(quebec.BaseClass):
        concurrency_limit = 2

        @staticmethod
        def concurrency_key(*args, **kwargs) -> str:
            return "resource-x"

        def perform(self, *args, **kwargs) -> None:
            pass

    qc.register_job(ProperJob)  # must not raise


# ──────────────────────────────────────────────────────────────────────────
# Layer 2 — runtime fallback to group-only key (no silent NULL)
# ──────────────────────────────────────────────────────────────────────────


def test_concurrency_key_returning_none_falls_back_to_group(qc_with_sqlalchemy):
    """Forgot `return` (Python defaults to None) → key falls back to group, NOT NULL."""
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class ForgotReturnJob(quebec.BaseClass):
        concurrency_limit = 1

        @staticmethod
        def concurrency_key(*args, **kwargs):
            # No return → Python returns None
            return None

        def perform(self, *args, **kwargs) -> None:
            pass

    qc.register_job(ForgotReturnJob)
    ForgotReturnJob.perform_later(qc, "x")

    row = session.execute(
        text(f"SELECT concurrency_key FROM {prefix}_jobs ORDER BY id DESC LIMIT 1")
    ).fetchone()
    # Fallback key is just the group (class name by default), no "/key" suffix.
    assert row.concurrency_key is not None
    assert row.concurrency_key.endswith("ForgotReturnJob")
    assert "/" not in row.concurrency_key


def test_concurrency_key_returning_empty_string_falls_back_to_group(qc_with_sqlalchemy):
    """Empty-string key → same fallback as None."""
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class EmptyKeyJob(quebec.BaseClass):
        concurrency_limit = 1

        @staticmethod
        def concurrency_key(*args, **kwargs) -> str:
            return ""

        def perform(self, *args, **kwargs) -> None:
            pass

    qc.register_job(EmptyKeyJob)
    EmptyKeyJob.perform_later(qc, "x")

    row = session.execute(
        text(f"SELECT concurrency_key FROM {prefix}_jobs ORDER BY id DESC LIMIT 1")
    ).fetchone()
    assert row.concurrency_key is not None
    assert row.concurrency_key.endswith("EmptyKeyJob")
    assert "/" not in row.concurrency_key


def test_concurrency_key_normal_path_unchanged(qc_with_sqlalchemy):
    """Non-empty key → "group/key" composition unchanged (regression guard)."""
    qc = qc_with_sqlalchemy["qc"]
    session = qc_with_sqlalchemy["session"]
    prefix = qc_with_sqlalchemy["prefix"]

    class NormalJob(quebec.BaseClass):
        concurrency_limit = 1

        @staticmethod
        def concurrency_key(*args, **kwargs) -> str:
            return "tenant-42"

        def perform(self, *args, **kwargs) -> None:
            pass

    qc.register_job(NormalJob)
    NormalJob.perform_later(qc, "x")

    row = session.execute(
        text(f"SELECT concurrency_key FROM {prefix}_jobs ORDER BY id DESC LIMIT 1")
    ).fetchone()
    assert row.concurrency_key is not None
    assert row.concurrency_key.endswith("NormalJob/tenant-42")
