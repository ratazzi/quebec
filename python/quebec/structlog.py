"""Structlog integration for quebec.

Plug ``add_quebec_context`` into your own structlog configuration so the
per-job context set by the quebec worker shows up in your logs.

Example:

    import structlog
    import quebec.structlog

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            quebec.structlog.add_quebec_context,
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
    )
"""

from .context import job_context_var

__all__ = ["add_quebec_context"]


def add_quebec_context(logger, method_name, event_dict):
    """Add quebec's JobContext fields (jid, queue, target) to the event dict.

    Reads from the contextvar set by the quebec worker around each job
    execution. Outside a worker the contextvar is empty, so this is a no-op.
    Existing keys in ``event_dict`` are never overwritten.
    """
    ctx = job_context_var.get()
    if "jid" not in event_dict and ctx.jid:
        event_dict["jid"] = ctx.jid
    if "queue" not in event_dict and ctx.queue:
        event_dict["queue"] = ctx.queue
    if "target" not in event_dict and ctx.target:
        event_dict["target"] = ctx.target
    return event_dict
