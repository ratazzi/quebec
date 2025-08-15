import logging
import contextvars
from datetime import datetime, timezone
from typing import Optional

job_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("job_id", default=None)

class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.job_id = job_id_var.get(None)
        except Exception:
            record.job_id = None
        return True

class QuebecFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def format(self, record: logging.LogRecord) -> str:
        record.asctime = self.formatTime(record)
        record.message = record.getMessage()
        jid = getattr(record, 'job_id', None)
        ctx = f" [jid={jid}]" if jid not in (None, '', '-') else ""
        origin = f"[{record.name}:{record.filename}:{record.lineno}:{record.thread}]"
        return f"{record.asctime} {record.levelname:>5}{ctx} {origin} {record.message}"


def setup_logging(level: int = logging.INFO, *, replace_root: bool = True) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    if replace_root:
        root.handlers[:] = []

    root.addFilter(ContextFilter())
    handler = logging.StreamHandler()
    handler.setFormatter(QuebecFormatter())
    handler.addFilter(ContextFilter())
    root.addHandler(handler)
