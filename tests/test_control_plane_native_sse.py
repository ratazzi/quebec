"""The native Axum control plane streams a real first stats event."""

import http.client
import socket
import time


def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def test_native_events_stream_stats(qc):
    port = available_port()
    qc.start_control_plane(f"127.0.0.1:{port}")
    deadline = time.perf_counter() + 5
    while True:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        try:
            connection.request("GET", "/health")
            response = connection.getresponse()
            response.read()
            if response.status == 200:
                break
        except OSError:
            if time.perf_counter() >= deadline:
                raise
            time.sleep(0.02)
        finally:
            connection.close()

    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request("GET", "/events")
        response = connection.getresponse()
        assert response.status == 200
        assert "text/event-stream" in response.getheader("Content-Type", "")
        lines = []
        while True:
            line = response.readline()
            assert line, "SSE stream ended before its first event"
            if line in (b"\n", b"\r\n"):
                break
            lines.append(line)
        event = b"".join(lines)
        assert b"event: message" in event
        assert b"<turbo-stream" in event
    finally:
        connection.close()
