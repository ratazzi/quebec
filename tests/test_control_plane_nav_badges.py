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


class NavWork(quebec.BaseClass):
    queue_as = "perf"

    def perform(self, value):
        return None


class OtherNavWork(quebec.BaseClass):
    queue_as = "other"

    def perform(self, value):
        return None


def _get(qc, path: str, query: str = "") -> tuple[int, str]:
    req = quebec.AsgiRequest("GET", path, query, [], b"", BASE)
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
        assert payload.strip().isdigit(), (
            f"{target} should stream a number: {payload!r}"
        )


def test_stats_stream_keeps_queue_and_scheduled_counts(qc) -> None:
    qc.register_job(NavWork)
    qc.perform_all_later([NavWork.build(1), NavWork.set(wait=3600).build(2)])

    status, body = _get(qc, "/stats")
    assert status == 200
    assert re.search(
        r'<turbo-stream action="update" target="scheduled-jobs-count">\s*'
        r"<template>1</template>",
        body,
    )
    assert re.search(
        r'<turbo-stream action="update" target="queue-count-perf">\s*'
        r"<template>\s*<div[^>]*>1</div>",
        body,
    )


def test_queues_page_keeps_unfiltered_nav_snapshot(qc) -> None:
    qc.register_job(NavWork)
    qc.register_job(OtherNavWork)
    qc.perform_all_later([NavWork.build(1), OtherNavWork.build(2)])
    assert qc.pause_queue("other") is True

    status, body = _get(qc, "/queues", "status=paused")
    assert status == 200
    assert 'id="queue-count-other"' in body
    assert 'id="queue-count-perf"' not in body
    assert re.search(r'id="queue-count-other"[^>]*>\s*<div[^>]*>1</div>', body)
    assert re.search(r'id="queue-status-other"[^>]*>\s*<span[^>]*>\s*Paused', body)

    status, stream = _get(qc, "/stats")
    assert status == 200
    assert 'target="queue-count-other"' in stream
    assert 'target="queue-count-perf"' in stream


def test_queue_detail_keeps_count_and_pause_status(qc) -> None:
    qc.register_job(NavWork)
    qc.register_job(OtherNavWork)
    qc.perform_all_later(
        [NavWork.build(i) for i in range(12)] + [OtherNavWork.build(1)]
    )
    assert qc.pause_queue("perf") is True

    status, body = _get(qc, "/queues/perf")
    assert status == 200
    assert "Queue: perf" in body
    assert "Status: <span" in body and ">paused</span>" in body
    assert "/queues/perf/resume" in body
    assert "?page=2" in body

    status, next_page = _get(qc, "/queues/perf", "page=2")
    assert status == 200
    assert "Queue: perf" in next_page
    assert "No ready jobs in this queue" not in next_page


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
