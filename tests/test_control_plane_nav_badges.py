"""The nav count badges.

`/stats` refreshes them over Turbo Streams. `action="update"` replaces the
target's *inner* HTML, so the stream must carry the bare number: wrapping it in
another badge span nests two of them and the pill visibly doubles in width
after the first refresh, while entries without a stream keep the correct size.
"""

from __future__ import annotations

import re

import pytest
import quebec

BASE = "/quebec"

#: Every badge, server-rendered and streamed, must use exactly this styling.
BADGE_CLASS = "px-1 py-0.5 text-xs rounded-full bg-gray-200 text-gray-800"


def _get(qc, path: str) -> tuple[int, str]:
    req = quebec.AsgiRequest("GET", path, "", [], b"", BASE)
    status, _headers, body = qc.handle_control_plane_request(req)
    return status, bytes(body).decode()


@pytest.fixture
def qc(qc_with_sqlalchemy):
    return qc_with_sqlalchemy["qc"]


def test_count_streams_carry_the_number_alone(qc) -> None:
    status, body = _get(qc, "/stats")

    assert status == 200
    streams = re.findall(
        r'<turbo-stream action="update" target="([a-z-]+-count)">\s*'
        r"<template>(.*?)</template>",
        body,
        re.S,
    )
    assert streams, "no count streams rendered"
    for target, payload in streams:
        assert "<span" not in payload, (
            f"{target} streams a badge into a badge; the update replaces inner "
            f"HTML, so this nests two pills: {payload!r}"
        )
        assert payload.strip().isdigit(), f"{target} should stream a number: {payload!r}"


def test_every_rendered_badge_is_also_refreshed(qc) -> None:
    """A badge the stream forgets keeps its page-load value forever. Batches
    was in exactly that state, which is also what made the old double-padding
    visible: it was the only one the refresh never touched."""
    _status, page = _get(qc, "/")
    _status, stream = _get(qc, "/stats")

    rendered = set(re.findall(r'<span id="([a-z-]+-count)"', page))
    refreshed = set(
        re.findall(r'<turbo-stream action="update" target="([a-z-]+-count)"', stream)
    )

    assert rendered, "no nav badges rendered"
    assert rendered <= refreshed, f"never refreshed: {sorted(rendered - refreshed)}"


def test_every_nav_badge_shares_one_style(qc) -> None:
    """A nav entry whose count is not streamed (Batches) must still look the
    same as the streamed ones."""
    status, body = _get(qc, "/")

    assert status == 200
    badges = re.findall(r'<span id="([a-z-]+-count)" class="([^"]*)"', body)
    assert badges, "no nav badges rendered"
    for name, classes in badges:
        assert classes == BADGE_CLASS, f"{name} deviates from the badge styling"
