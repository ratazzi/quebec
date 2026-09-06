"""Builder options must not become arguments to user concurrency callbacks."""

import json
from datetime import datetime, timedelta, timezone

import pytest
import quebec

from .helpers import get_job_by_active_job_id


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("option", [None, "queue", "priority", "wait", "wait_until"])
def test_builder_options_stay_out_of_concurrency_key(qc_with_sqlalchemy, db_assert, bulk, option):
    qc = qc_with_sqlalchemy["qc"]
    seen = []

    class BuilderJob(quebec.BaseClass):
        concurrency_limit = 1

        @staticmethod
        def queue_as(value, *, _id):
            return "default"

        def concurrency_key(self, value, *, _id):
            seen.append((value, _id))
            return f"{_id}:{value}"

        def perform(self, value, *, _id):
            pass

    qc.register_job(BuilderJob)
    options = {
        "queue": {"queue": "other"},
        "priority": {"priority": 7},
        "wait": {"wait": 3600},
        "wait_until": {"wait_until": datetime.now(timezone.utc) + timedelta(hours=1)},
    }.get(option, {})
    builder = BuilderJob.set(**options)
    if bulk:
        job = qc.perform_all_later([builder.build(42, _id="tenant")])[0]
    else:
        job = builder.perform_later(qc, 42, _id="tenant")

    assert seen == [(42, "tenant")]
    persisted = get_job_by_active_job_id(
        qc_with_sqlalchemy["session"], qc_with_sqlalchemy["prefix"], job.active_job_id
    )
    assert persisted["concurrency_key"].endswith("/tenant:42")
    assert persisted["queue_name"] == options.get("queue", "default")
    assert persisted["priority"] == options.get("priority", 0)
    payload = json.loads(persisted["arguments"])
    arguments = payload["arguments"] if bulk else payload["arguments"]["arguments"]
    assert arguments == [
        42, {"_id": "tenant", "_quebec_kwargs": True}
    ]
    delayed = option in ("wait", "wait_until")
    assert db_assert.count_scheduled_executions() == int(delayed)
    assert db_assert.count_ready_executions() == int(not delayed)
