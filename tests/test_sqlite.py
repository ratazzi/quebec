import os
import tempfile

import quebec


def test_memory():
    qc = quebec.Quebec("sqlite::memory:")
    assert qc.ping() is True
    assert qc.create_tables() is True
    qc.close()


def test_file():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        qc = quebec.Quebec(f"sqlite://{path}?mode=rwc")
        assert qc.ping() is True
        assert qc.create_tables() is True
        qc.close()
    finally:
        os.unlink(path)
