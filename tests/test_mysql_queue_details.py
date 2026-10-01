"""Queue details must use the same collation as their SQL job listing."""

import os
import uuid
from urllib.parse import unquote, urlsplit

import pytest
import quebec


class MySqlMixedCaseQueueWork(quebec.BaseClass):
    queue_as = "Email"

    def perform(self, value):
        return None


@pytest.mark.skipif(
    not os.getenv("TEST_MYSQL_URL"), reason="requires disposable TEST_MYSQL_URL"
)
def test_mysql_queue_detail_preserves_collation_for_count_and_pause_status():
    pymysql = pytest.importorskip("pymysql")
    dsn = os.environ["TEST_MYSQL_URL"]
    parsed = urlsplit(dsn)
    prefix = f"qd_{uuid.uuid4().hex[:10]}"
    qc = quebec.Quebec(dsn, table_name_prefix=prefix, use_listen_notify=False)
    observer = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        autocommit=True,
    )
    try:
        qc.create_tables()
        # Make the regression deterministic regardless of the database default.
        with observer.cursor() as cursor:
            for table in ("jobs", "ready_executions", "pauses"):
                cursor.execute(
                    f"ALTER TABLE {prefix}_{table} MODIFY queue_name "
                    "VARCHAR(255) CHARACTER SET utf8mb4 "
                    "COLLATE utf8mb4_unicode_ci NOT NULL"
                )
        qc.register_job(MySqlMixedCaseQueueWork)
        qc.perform_all_later([MySqlMixedCaseQueueWork.build(i) for i in range(12)])
        assert qc.pause_queue("Email") is True

        for queue_name in ("Email", "email", "Émail"):
            path = f"/queues/{queue_name}"
            for page in (1, 2):
                request = quebec.AsgiRequest(
                    "GET", path, f"page={page}", [], b"", "/quebec"
                )
                status, _headers, body = qc.handle_control_plane_request(request)
                assert status == 200
                rendered = bytes(body).decode()
                assert ">paused</span>" in rendered
                assert f"{path}/resume" in rendered
                if page == 1:
                    assert "?page=2" in rendered
                assert "No ready jobs in this queue" not in rendered
    finally:
        observer.close()
        qc.close()
