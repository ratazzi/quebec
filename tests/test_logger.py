"""Tests for quebec.logger.

Traceback rendering: setup_structlog must render exception tracebacks in
every format. Before the fix the processors chain had no exception
processor, so ``log.exception(...)`` dropped the traceback and only emitted
a bare ``exc_info`` flag.
"""

import json
import logging

from quebec.logger import setup_structlog, get_structlog


def _emit_exception(capsys, message="job failed", **kwargs):
    setup_structlog(level=logging.DEBUG, **kwargs)
    log = get_structlog("regression")
    try:
        raise ValueError("kaboom")
    except ValueError:
        log.exception(message)
    return capsys.readouterr().out


def test_console_renders_traceback(capsys):
    out = _emit_exception(capsys, format="console")
    assert "Traceback (most recent call last):" in out
    assert "ValueError: kaboom" in out
    # No bare flag leaking through as a key-value pair.
    assert "exc_info" not in out


def test_logfmt_renders_traceback(capsys):
    out = _emit_exception(capsys, format="logfmt")
    assert "exception=" in out
    assert "ValueError: kaboom" in out


def test_json_renders_structured_traceback(capsys):
    out = _emit_exception(capsys, format="json").strip().splitlines()[-1]
    record = json.loads(out)
    assert record["exception"][0]["exc_type"] == "ValueError"
    assert record["exception"][0]["exc_value"] == "kaboom"


def _json_frames(capsys, **kwargs):
    setup_structlog(level=logging.DEBUG, format="json", **kwargs)
    log = get_structlog("regression")

    def boom():
        secret = "s3cr3t"  # noqa: F841 - intentionally present in the frame
        raise RuntimeError("nope")

    try:
        boom()
    except RuntimeError:
        log.exception("failed")
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    return record["exception"][0]["frames"]


def test_json_hides_locals_by_default(capsys):
    frames = _json_frames(capsys)
    assert all("locals" not in frame for frame in frames)


def test_json_show_locals_opt_in(capsys):
    frames = _json_frames(capsys, show_locals=True)
    assert any(frame.get("locals", {}).get("secret") == "'s3cr3t'" for frame in frames)
