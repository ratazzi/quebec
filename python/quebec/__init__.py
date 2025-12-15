from .quebec import * # NOQA
import logging
import time
import queue
import threading
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Callable, List, Tuple, Type, Any, Optional, Union
import dataclasses
from .logger import job_id_var

__doc__ = quebec.__doc__
if hasattr(quebec, "__all__"):
    __all__ = quebec.__all__

logger = logging.getLogger(__name__)


class JobBuilder:
    """Builder for configuring job options before enqueueing.

    This allows chaining configuration like:
        MyJob.set(wait=3600).perform_later(qc, arg1, arg2)
        MyJob.set(queue='high', priority=10).perform_later(qc, arg1)
    """

    def __init__(self, job_class: Type, **options):
        self.job_class = job_class
        self.options = options

    def _calculate_scheduled_at(self) -> Optional[datetime]:
        """Calculate scheduled_at from wait or wait_until options."""
        wait = self.options.get('wait')
        wait_until = self.options.get('wait_until')

        if wait_until is not None:
            if isinstance(wait_until, datetime):
                # Ensure timezone-aware for correct timestamp conversion
                if wait_until.tzinfo is None:
                    # Assume naive datetime is UTC
                    wait_until = wait_until.replace(tzinfo=timezone.utc)
                return wait_until
            raise ValueError("wait_until must be a datetime object")

        if wait is not None:
            # Use timezone-aware UTC datetime
            now = datetime.now(timezone.utc)
            if isinstance(wait, (int, float)):
                return now + timedelta(seconds=wait)
            elif isinstance(wait, timedelta):
                return now + wait
            raise ValueError("wait must be a number (seconds) or timedelta")

        return None

    def perform_later(self, qc: 'Quebec', *args, **kwargs) -> 'ActiveJob':
        """Enqueue the job with configured options."""
        scheduled_at = self._calculate_scheduled_at()

        # Pass internal options via kwargs (will be filtered out before serialization)
        if scheduled_at is not None:
            kwargs['_scheduled_at'] = scheduled_at.timestamp()

        if 'queue' in self.options:
            kwargs['_queue'] = self.options['queue']

        if 'priority' in self.options:
            kwargs['_priority'] = self.options['priority']

        # Call the original perform_later
        return self.job_class.perform_later(qc, *args, **kwargs)


class TestJob:
    def perform(self):
        pass

# @dataclasses.dataclass
# class RetryStrategy:
#     wait: int = 3 # seconds
#     attempts: int = 5 # attempts
#     # exceptions: tuple = (Exception,)

class NoNewOverrideMeta(type):
    def __new__(cls, name, bases, dct):
        if '__new__' in dct:
            raise TypeError(f"Overriding __new__ is not allowed in class {name}")
        if '__init__' in dct:
            raise TypeError(f"Overriding __init__ is not allowed in class {name}")
        return super().__new__(cls, name, bases, dct)

class BaseClass(ActiveJob, metaclass=NoNewOverrideMeta):
    @classmethod
    def set(cls, wait: Union[int, float, timedelta] = None,
            wait_until: datetime = None,
            queue: str = None,
            priority: int = None) -> JobBuilder:
        """Configure job options before enqueueing.

        Args:
            wait: Delay in seconds (int/float) or timedelta before running
            wait_until: Specific datetime when the job should run
            queue: Queue name to enqueue the job to
            priority: Job priority (lower number = higher priority)

        Returns:
            JobBuilder instance for chaining with perform_later

        Example:
            MyJob.set(wait=3600).perform_later(qc, arg1)  # Run in 1 hour
            MyJob.set(wait_until=tomorrow).perform_later(qc, arg1)
            MyJob.set(queue='critical', priority=1).perform_later(qc, arg1)
        """
        options = {}
        if wait is not None:
            options['wait'] = wait
        if wait_until is not None:
            options['wait_until'] = wait_until
        if queue is not None:
            options['queue'] = queue
        if priority is not None:
            options['priority'] = priority
        return JobBuilder(cls, **options)


class ThreadedRunner:
    def __init__(self, queue: queue.Queue, event: threading.Event):
        self.queue = queue
        self.event = event
        self.execution: Optional[Any] = None

    def run(self):
        """Main loop for processing jobs"""
        while not self.event.is_set():
            try:
                self.execution = self.queue.get(timeout=0.1)
                if self.execution is None:
                    continue

                self.queue.task_done()
                self.execution.tid = str(threading.get_ident())

                # Inject jid (active_job_id) into the context before execution, clean up after execution.
                token = job_id_var.set(self.execution.jid)
                self.execution.perform()
                logger.debug(str(self.execution.metric) + "\n")
                job_id_var.reset(token)
            except queue.Empty:
                time.sleep(0.1)
            except (queue.ShutDown, KeyboardInterrupt) as e:
                break
            except Exception as e:
                logger.error(f"Unexpected exception in ThreadedRunner: {e}", exc_info=True)
            finally:
                self.cleanup()

        logger.debug("threaded_runner exit")

    def cleanup(self):
        """Cleanup after job execution"""
        try:
            if self.execution and hasattr(self.execution, 'cleanup'):
                self.execution.cleanup()
        except Exception as e:
            logger.error(f"Error in cleanup: {e}", exc_info=True)

# def rescue_from(func):
#     @wraps(func)
#     def wrapper(self, *args, **kwargs):
#         # 这里可以访问到 self
#         print(f"Before calling {func.__name__}")
#         result = func(self, *args, **kwargs)
#         print(f"After calling {func.__name__}")
#         return result
#     return wrapper

# def rescue_from(*exceptions: Type[Exception]):
#     print(f">>>>>>>>>>>>> {exceptions}")
#     def decorator(func: Callable[[Any, Exception], None]):
#         @wraps(func)
#         def wrapper(self, *args, **kwargs):
#             print(f"registering {self} with {exceptions}")
#             print(self.rescue_strategies)
#             return func(self, *args, **kwargs)
#         # cls.rescue_strategies.append(RescueStrategy(exceptions, func))
#         print(decorator.__class__)
#         # print(dir(func))
#         # print(dir(wrapper))
#         print(f"--------------------------- rescue_from {exceptions} {func}")
#         return wrapper
#     return decorator
