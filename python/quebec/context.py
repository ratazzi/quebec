"""Per-job context shared between the worker, logger, and structlog integration.

Kept in its own module so that ``quebec.logger`` and ``quebec.structlog`` can
both depend on it without importing each other.
"""

import contextvars
from dataclasses import dataclass

__all__ = ["JobContext", "job_context_var", "current_batch_id"]


@dataclass(frozen=True)
class JobContext:
    jid: str | None = None
    queue: str | None = None
    target: str | None = None


job_context_var: contextvars.ContextVar[JobContext] = contextvars.ContextVar(
    "job_context", default=JobContext()
)

# The batch jobs enqueued in this context join. Set by ``with qc.batch(...)``
# / ``with batch.enqueue()`` and read by the Rust enqueue path, so it works
# for ``perform_later`` and ``perform_all_later`` alike.
current_batch_id: contextvars.ContextVar["int | None"] = contextvars.ContextVar(
    "quebec_current_batch_id", default=None
)
