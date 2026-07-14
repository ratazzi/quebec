"""queue.yml `concurrency_maintenance: false` disables the dispatcher's
concurrency-maintenance sweep (previously parsed but never read)."""

from __future__ import annotations

import quebec


def _queue_yml(tmp_path, body: str) -> str:
    path = tmp_path / "queue.yml"
    path.write_text(body)
    return str(path)


def test_concurrency_maintenance_defaults_true(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.delenv("QUEBEC_CONFIG", raising=False)
    monkeypatch.delenv("QUEBEC_ENV", raising=False)
    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert inst._dispatcher_concurrency_maintenance() is True
    finally:
        inst.close()


def test_concurrency_maintenance_false_is_honored(
    tmp_path, monkeypatch, db_url, test_prefix
) -> None:
    monkeypatch.setenv(
        "QUEBEC_CONFIG",
        _queue_yml(
            tmp_path,
            """
development:
  dispatchers:
    - concurrency_maintenance: false
""",
        ),
    )
    monkeypatch.delenv("QUEBEC_ENV", raising=False)
    inst = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    try:
        assert inst._dispatcher_concurrency_maintenance() is False
    finally:
        inst.close()
