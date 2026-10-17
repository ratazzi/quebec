"""close() releases the instance: registering a job class must not pin it.

`register_job` binds `cls.quebec` to the instance while the instance's registry
holds the class. That cycle runs through Rust, out of the GC's sight, so unless
close() breaks it the instance — and its tokio runtime's threads and fds —
lives until the process exits.
"""

from __future__ import annotations

import gc
import os

import pytest
import quebec


def _job_class():
    class Job(quebec.BaseClass):
        def perform(self, *args, **kwargs):
            pass

    return Job


def test_close_unbinds_registered_job_classes(db_url, test_prefix):
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    job = _job_class()
    qc.register_job(job)
    assert "quebec" in job.__dict__

    qc.close()

    assert "quebec" not in job.__dict__


def test_close_keeps_a_class_bound_to_a_newer_instance(db_url, test_prefix):
    first = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    second = quebec.Quebec(db_url, table_name_prefix=test_prefix)
    job = _job_class()
    first.register_job(job)
    second.register_job(job)

    first.close()
    assert "quebec" in job.__dict__

    second.close()
    assert "quebec" not in job.__dict__


def test_rejected_job_class_is_not_bound(db_url, test_prefix):
    qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)

    class MissingDuration(quebec.BaseClass):
        rate_limit_max = 5

        def perform(self, *args, **kwargs):
            pass

    with pytest.raises(ValueError, match="rate_limit_duration"):
        qc.register_job(MissingDuration)

    assert "quebec" not in MissingDuration.__dict__
    qc.close()


@pytest.mark.skipif(not os.path.isdir("/dev/fd"), reason="needs /dev/fd")
def test_closed_instances_with_registered_jobs_free_their_fds(db_url, test_prefix):
    def cycle():
        qc = quebec.Quebec(db_url, table_name_prefix=test_prefix)
        qc.register_job(_job_class())
        qc.close()

    # Warm up one-time process state (logging, driver statics) first.
    cycle()
    gc.collect()
    before = len(os.listdir("/dev/fd"))

    for _ in range(5):
        cycle()
    gc.collect()

    # Each leaked instance held its runtime's driver fds (kqueue/epoll and
    # friends), so five leaks would show up as at least five extra fds.
    assert len(os.listdir("/dev/fd")) - before < 5
