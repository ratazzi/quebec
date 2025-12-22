# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "quebec",
# ]
#
# [tool.uv.sources]
# quebec = { git = "https://github.com/ratazzi/quebec.git", rev = "master" }
# ///
import sys
import logging
from pathlib import Path

from quebec.logger import setup_logging
setup_logging(level=logging.DEBUG)
logger = logging.getLogger()

import quebec

db_path = Path(len(sys.argv) > 1 and sys.argv[1] or 'demo.db')
qc = quebec.Quebec(f'sqlite://{db_path}?mode=rwc', use_skip_locked=False)


@qc.register_job
class FakeJob(quebec.BaseClass):
    def perform(self, *args, **kwargs):
        self.logger.info(f">>> {self.id}, args: {args}, kwargs: {kwargs}")
        self.logger.debug("via rust tracing logger")


if __name__ == "__main__":
    FakeJob.perform_later(qc, 3466, foo='bar')

    qc.run(
        create_tables=not db_path.exists(),
        control_plane='127.0.0.1:5006',
    )
