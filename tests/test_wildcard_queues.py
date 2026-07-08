"""Wildcard queue pattern matching (`order_*` style prefixes).

Solid Queue treats wildcard patterns as plain string prefixes. The SQL LIKE
translation must escape `_`/`%` so `order_*` does not also match `orders`.
"""

from __future__ import annotations

import pytest

import quebec


class WildcardJob(quebec.BaseClass):
    def perform(self, *args, **kwargs) -> None:
        pass


@pytest.fixture
def wildcard_qc(tmp_path, monkeypatch, temp_db_path, test_prefix):
    queue_yml = tmp_path / "queue.yml"
    queue_yml.write_text(
        """
development:
  workers:
    - queues: "order_*"
      threads: 1
"""
    )
    monkeypatch.setenv("QUEBEC_CONFIG", str(queue_yml))
    monkeypatch.delenv("QUEBEC_ENV", raising=False)

    qc = quebec.Quebec(
        f"sqlite:///{temp_db_path}?mode=rwc", table_name_prefix=test_prefix
    )
    assert qc.create_tables() is True
    qc.register_job(WildcardJob)

    yield qc

    qc.close()


def test_wildcard_prefix_is_literal_not_like_pattern(wildcard_qc) -> None:
    # `_` is a LIKE metacharacter matching any single character; without
    # escaping, the `order_*` worker would also claim from `orders`.
    WildcardJob.set(queue="orders").perform_later(wildcard_qc)
    WildcardJob.set(queue="order_a").perform_later(wildcard_qc)

    execution = wildcard_qc.drain_one()
    assert execution.queue == "order_a"
    execution.perform()

    with pytest.raises(RuntimeError):
        wildcard_qc.drain_one()
