import logging
import contextvars
from datetime import datetime, timezone
from typing import Optional

job_id_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "job_id", default=None
)
queue_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "queue", default=None
)


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.job_id = job_id_var.get(None)
            record.queue = queue_var.get(None)
        except Exception:
            record.job_id = None
            record.queue = None
        return True


class QuebecFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def format(self, record: logging.LogRecord) -> str:
        record.asctime = self.formatTime(record)
        record.message = record.getMessage()
        jid = getattr(record, "job_id", None)
        queue = getattr(record, "queue", None)
        if jid not in (None, "", "-"):
            ctx = f' {{queue="{queue}" jid="{jid}" tid="{record.thread}"}}:'
        else:
            ctx = ""
        origin = f"{record.name}:{record.filename}: {record.lineno}:"
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


def _add_job_id(logger, method_name, event_dict):
    """Processor to add job_id from contextvars."""
    jid = job_id_var.get(None)
    if jid:
        event_dict["jid"] = jid
    return event_dict


def _rename_event_to_message(logger, method_name, event_dict):
    """Rename 'event' to 'message' for Rust tracing-logfmt compatibility."""
    if "event" in event_dict:
        event_dict["message"] = event_dict.pop("event")
    return event_dict


# Module-level flag to prevent duplicate warnings
_styles_warning_shown = False


class TracingConsoleRenderer:
    """Renderer matching Rust tracing console format.

    Format: 2026-01-20T14:22:53Z  INFO target: lineno: message key=value
    """

    def __init__(self, colors: bool = True):
        import os
        import sys

        self._use_colors = os.environ.get("QUEBEC_COLOR") == "always" or (
            os.environ.get("QUEBEC_COLOR") != "never" and colors and sys.stdout.isatty()
        )
        self._styles = self._load_styles() if self._use_colors else None

    def _load_styles(self) -> Optional[dict]:
        """Load styles from structlog's ConsoleRenderer.

        Note: Uses private attributes (_styles, _level_styles) which may change
        in future structlog versions. Falls back to plain output if unavailable.
        """
        try:
            import structlog

            cr = structlog.dev.ConsoleRenderer(colors=True)
            return {
                "reset": cr._styles.reset,
                "dim": cr._styles.timestamp,
                "bright": cr._styles.bright,
                "levels": cr._level_styles,
            }
        except (AttributeError, Exception) as e:
            global _styles_warning_shown
            if not _styles_warning_shown:
                import sys

                print(
                    f"quebec: failed to load structlog color styles ({e}), falling back to plain output",
                    file=sys.stderr,
                )
                _styles_warning_shown = True
            return None

    def __call__(self, logger, method_name, event_dict):
        timestamp = event_dict.pop("timestamp", "")
        level = event_dict.pop("level", "info").upper()
        event = event_dict.pop("event", "")
        lineno = event_dict.pop("lineno", "")
        target = event_dict.pop("target", "quebec")

        extra = self._format_kv(event_dict)
        lineno_part = f"{lineno}: " if lineno else ""

        if self._styles:
            s = self._styles
            ts = f"{s['dim']}{timestamp}{s['reset']}"
            lvl_style = s["levels"].get(method_name, "")
            lvl = f"{lvl_style}{level:>5}{s['reset']}"
            return f"{ts} {lvl} {target}: {lineno_part}{event}{extra}"

        return f"{timestamp} {level:>5} {target}: {lineno_part}{event}{extra}"

    @staticmethod
    def _format_kv(event_dict: dict) -> str:
        """Format key-value pairs for console output.

        Escapes quotes and handles values with whitespace per logfmt conventions.
        """
        if not event_dict:
            return ""
        parts = []
        for k, v in event_dict.items():
            if isinstance(v, str):
                # Escape internal quotes and wrap in quotes if contains whitespace/quotes
                escaped = v.replace("\\", "\\\\").replace('"', '\\"')
                parts.append(f'{k}="{escaped}"')
            else:
                parts.append(f"{k}={v}")
        return " " + " ".join(parts)


def setup_structlog(level: int = logging.INFO, *, format: str | None = None) -> None:
    """Configure structlog with job_id context support.

    Args:
        level: Logging level (default INFO)
        format: Output format - "console" (colored), "json", or "logfmt".
                Defaults to QUEBEC_LOG_FORMAT env var, or "console" if not set.

    Example:
        from quebec.logger import setup_structlog, get_structlog
        setup_structlog(level=logging.DEBUG)
        setup_structlog(format="logfmt")  # logfmt output
        setup_structlog(format="json")    # JSON output
        log = get_structlog()
        log.info("job started", queue="default")
    """
    import os

    if format is None:
        format = os.environ.get("QUEBEC_LOG_FORMAT", "console")
    try:
        import structlog
    except ImportError:
        raise ImportError("structlog is required. Install with: pip install structlog")

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        _add_job_id,
        structlog.processors.add_log_level,
        structlog.processors.CallsiteParameterAdder(
            [
                structlog.processors.CallsiteParameter.LINENO,
            ]
        ),
    ]

    if format == "json":
        processors = shared_processors + [
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    elif format == "logfmt":
        # Use ts=/message= to match Rust tracing-logfmt output
        processors = shared_processors + [
            structlog.processors.TimeStamper(fmt="iso", key="ts"),
            _rename_event_to_message,
            structlog.processors.LogfmtRenderer(
                key_order=["ts", "level", "message"],
                sort_keys=True,
            ),
        ]
    else:  # console
        processors = shared_processors + [
            structlog.processors.TimeStamper(fmt="iso"),
            TracingConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_structlog(name: Optional[str] = None):
    """Get a structlog logger instance.

    Example:
        log = get_structlog(__name__)
        log.info("processing", job_id=123, queue="default")
    """
    try:
        import structlog
    except ImportError:
        raise ImportError("structlog is required. Install with: pip install structlog")

    # Bind the logger name as 'target' to match Rust tracing output
    logger = structlog.get_logger()
    if name:
        logger = logger.bind(target=name)
    return logger
