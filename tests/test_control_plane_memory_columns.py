"""The workers page has to say where each memory figure comes from.

`Memory` and `cgroup` are different quantities, and on a forked supervisor the
cgroup one reads well below RSS: a child inherits its parent's pages
copy-on-write, and those stay charged to the cgroup the fork happened in. Side
by side and unlabelled the pair looks like a bug, so both headers name their
source and carry the explanation.
"""

from __future__ import annotations

import quebec


def _get(qc, path: str) -> tuple[int, str]:
    req = quebec.AsgiRequest("GET", path, "", [], b"", "")
    status, _headers, body = qc.handle_control_plane_request(req)
    return status, bytes(body).decode()


def test_headers_name_the_source_of_each_figure(qc):
    status, html = _get(qc, "/workers")

    assert status == 200
    assert "Memory (RSS)" in html
    assert "cgroup (current)" in html


def test_headers_explain_why_the_two_disagree(qc):
    """A label alone does not tell an operator why cgroup sits below RSS."""
    status, html = _get(qc, "/workers")

    assert status == 200
    assert "shared copy-on-write" in html
    assert "inherited" in html and "below RSS" in html
