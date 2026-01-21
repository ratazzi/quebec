"""SQLAlchemy models for Quebec database tables.

These models mirror the Rust SeaORM entities and can be used for querying
the Quebec database directly from Python using SQLAlchemy.

Example usage:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from quebec.models import Job, ReadyExecution

    engine = create_engine("postgresql://localhost/quebec")
    with Session(engine) as session:
        # Query ready jobs with their job details
        ready = session.query(ReadyExecution).join(Job).filter(
            ReadyExecution.queue_name == "default"
        ).all()

        for r in ready:
            print(f"Job {r.job.class_name} ready in queue {r.queue_name}")
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all Quebec models."""

    pass


class Job(Base):
    """Main job table containing job definitions and state.

    Relationships:
        - ready_execution: One-to-one with ReadyExecution
        - claimed_execution: One-to-one with ClaimedExecution
        - blocked_execution: One-to-one with BlockedExecution
        - scheduled_execution: One-to-one with ScheduledExecution
        - failed_execution: One-to-one with FailedExecution
        - recurring_execution: One-to-one with RecurringExecution
    """

    __tablename__ = "solid_queue_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False)
    class_name: Mapped[str] = mapped_column(String(255), nullable=False)
    arguments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    priority: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    failed_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    active_job_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    concurrency_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Relationships
    ready_execution: Mapped[Optional["ReadyExecution"]] = relationship(
        back_populates="job", uselist=False, cascade="all, delete-orphan"
    )
    claimed_execution: Mapped[Optional["ClaimedExecution"]] = relationship(
        back_populates="job", uselist=False, cascade="all, delete-orphan"
    )
    blocked_execution: Mapped[Optional["BlockedExecution"]] = relationship(
        back_populates="job", uselist=False, cascade="all, delete-orphan"
    )
    scheduled_execution: Mapped[Optional["ScheduledExecution"]] = relationship(
        back_populates="job", uselist=False, cascade="all, delete-orphan"
    )
    failed_execution: Mapped[Optional["FailedExecution"]] = relationship(
        back_populates="job", uselist=False, cascade="all, delete-orphan"
    )
    recurring_execution: Mapped[Optional["RecurringExecution"]] = relationship(
        back_populates="job", uselist=False, cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_solid_queue_jobs_queue_priority", "queue_name", "priority"),
        Index("idx_solid_queue_jobs_class_name", "class_name"),
        Index("idx_solid_queue_jobs_finished_at", "finished_at"),
    )

    def __repr__(self) -> str:
        return f"<Job(id={self.id}, class_name={self.class_name!r}, queue={self.queue_name!r})>"


class ReadyExecution(Base):
    """Jobs that are ready to be executed.

    Jobs move here when they are enqueued and ready for immediate execution.
    Workers claim jobs from this table using FOR UPDATE SKIP LOCKED.
    """

    __tablename__ = "solid_queue_ready_executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("solid_queue_jobs.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False)
    priority: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="ready_execution")

    __table_args__ = (
        Index(
            "idx_solid_queue_ready_executions_queue_priority",
            "queue_name",
            "priority",
        ),
    )

    def __repr__(self) -> str:
        return f"<ReadyExecution(id={self.id}, job_id={self.job_id}, queue={self.queue_name!r})>"


class ClaimedExecution(Base):
    """Jobs that are currently being executed by a worker.

    When a worker claims a job, it moves from ready_executions to here.
    The process_id links to the worker process that claimed the job.
    """

    __tablename__ = "solid_queue_claimed_executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("solid_queue_jobs.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    process_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="claimed_execution")

    def __repr__(self) -> str:
        return f"<ClaimedExecution(id={self.id}, job_id={self.job_id}, process_id={self.process_id})>"


class BlockedExecution(Base):
    """Jobs that are blocked due to concurrency limits.

    When a job has a concurrency_key and the semaphore limit is reached,
    the job is placed here until a slot becomes available.
    """

    __tablename__ = "solid_queue_blocked_executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("solid_queue_jobs.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False)
    priority: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    concurrency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="blocked_execution")

    __table_args__ = (
        Index(
            "idx_solid_queue_blocked_executions_concurrency_key",
            "concurrency_key",
        ),
        Index("idx_solid_queue_blocked_executions_expires_at", "expires_at"),
    )

    def __repr__(self) -> str:
        return f"<BlockedExecution(id={self.id}, job_id={self.job_id}, key={self.concurrency_key!r})>"


class ScheduledExecution(Base):
    """Jobs that are scheduled for future execution.

    Jobs with a scheduled_at time in the future are placed here.
    The dispatcher moves them to ready_executions when the time comes.
    """

    __tablename__ = "solid_queue_scheduled_executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("solid_queue_jobs.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False)
    priority: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    scheduled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="scheduled_execution")

    __table_args__ = (
        Index("idx_solid_queue_scheduled_executions_scheduled_at", "scheduled_at"),
    )

    def __repr__(self) -> str:
        return f"<ScheduledExecution(id={self.id}, job_id={self.job_id}, scheduled_at={self.scheduled_at})>"


class FailedExecution(Base):
    """Jobs that have failed execution.

    When a job fails and exhausts retries (or has no retry strategy),
    it is moved here with the error message.
    """

    __tablename__ = "solid_queue_failed_executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("solid_queue_jobs.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="failed_execution")

    def __repr__(self) -> str:
        return f"<FailedExecution(id={self.id}, job_id={self.job_id})>"


class RecurringExecution(Base):
    """Tracks which recurring task occurrences have been executed.

    Used to prevent duplicate execution of recurring jobs when
    multiple scheduler instances are running.
    """

    __tablename__ = "solid_queue_recurring_executions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("solid_queue_jobs.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    task_key: Mapped[str] = mapped_column(String(255), nullable=False)
    run_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    job: Mapped["Job"] = relationship(back_populates="recurring_execution")

    __table_args__ = (
        Index(
            "index_solid_queue_recurring_executions_on_task_key_and_run_at",
            "task_key",
            "run_at",
            unique=True,
        ),
    )

    def __repr__(self) -> str:
        return f"<RecurringExecution(id={self.id}, task_key={self.task_key!r}, run_at={self.run_at})>"


class RecurringTask(Base):
    """Recurring task definitions loaded from schedule configuration.

    Defines cron-like schedules for periodic job execution.
    """

    __tablename__ = "solid_queue_recurring_tasks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    schedule: Mapped[str] = mapped_column(String(255), nullable=False)
    command: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    class_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    arguments: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    queue_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    priority: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    static: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    def __repr__(self) -> str:
        return f"<RecurringTask(id={self.id}, key={self.key!r}, schedule={self.schedule!r})>"


class Process(Base):
    """Worker, dispatcher, and scheduler process registration.

    Each running process registers itself here and updates its heartbeat
    periodically. Used for process monitoring and orphan detection.
    """

    __tablename__ = "solid_queue_processes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    kind: Mapped[str] = mapped_column(String(255), nullable=False)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    supervisor_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    hostname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    metadata_: Mapped[Optional[str]] = mapped_column("metadata", Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    __table_args__ = (
        Index("idx_solid_queue_processes_kind", "kind"),
        Index("idx_solid_queue_processes_last_heartbeat", "last_heartbeat_at"),
    )

    def __repr__(self) -> str:
        return f"<Process(id={self.id}, kind={self.kind!r}, pid={self.pid}, hostname={self.hostname!r})>"


class Semaphore(Base):
    """Concurrency semaphores for limiting parallel job execution.

    Each unique concurrency_key has a semaphore with a value (available slots)
    and a limit. When value reaches 0, jobs with that key are blocked.
    """

    __tablename__ = "solid_queue_semaphores"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    value: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    __table_args__ = (Index("idx_solid_queue_semaphores_expires_at", "expires_at"),)

    def __repr__(self) -> str:
        return f"<Semaphore(id={self.id}, key={self.key!r}, value={self.value})>"


class Pause(Base):
    """Queue pause state.

    When a queue is paused, no jobs from that queue will be processed
    until the pause is removed.
    """

    __tablename__ = "solid_queue_pauses"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    queue_name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    def __repr__(self) -> str:
        return f"<Pause(id={self.id}, queue_name={self.queue_name!r})>"


# Convenience exports
__all__ = [
    "Base",
    "Job",
    "ReadyExecution",
    "ClaimedExecution",
    "BlockedExecution",
    "ScheduledExecution",
    "FailedExecution",
    "RecurringExecution",
    "RecurringTask",
    "Process",
    "Semaphore",
    "Pause",
]
