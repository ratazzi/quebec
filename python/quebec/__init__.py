from .quebec import * # NOQA
import logging
import time
import queue
import threading
from functools import wraps
from typing import Callable, List, Tuple, Type, Any
import dataclasses

__doc__ = quebec.__doc__
if hasattr(quebec, "__all__"):
    __all__ = quebec.__all__

logger = logging.getLogger(__name__)


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
    def rescue_from(cls, *exceptions):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                # print(f"........................... wrapper: self: {self}, args: {args}, kwargs: {kwargs}")
                return func(self, *args, **kwargs)
            cls.register_rescue_strategy(RescueStrategy(exceptions, wrapper))
            return wrapper
        return decorator


class ThreadedRunner:
    def __init__(self, queue: queue.Queue, event: threading.Event):
        self.queue = queue
        self.event = event
        self.failed_attempts = 0
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

                self.execution.perform()
                logger.debug(self.execution.metric)

            except queue.Empty:
                time.sleep(0.1)
            except (queue.ShutDown, KeyboardInterrupt) as e:
                break
            except Exception as e:
                self.handle_exception(e)
            finally:
                self.cleanup()

        logger.debug("threaded_runner exit")

    def handle_exception(self, exception: Exception):
        """Handle different types of exceptions"""
        try:
            # if self.should_discard(exception):
            #     return

            # if self.should_rescue(exception):
            #     return

            if self.should_retry(exception):
                return

            # If none of the above handlers processed the exception
            logger.error(f"Unhandled exception: {exception}", exc_info=True)

        except Exception as e:
            logger.error(f"Error in exception handling: {e}", exc_info=True)

    def should_discard(self, exception: Exception) -> bool:
        """Apply discard_on strategy"""
        if not hasattr(self.execution.runnable.handler, 'discard_on'):
            return False

        for strategy in self.execution.runnable.handler.discard_on:
            if isinstance(exception, strategy.exceptions):
                logger.debug(f"Discarding job due to {exception.__class__.__name__}")
                return True
        return False

    def should_rescue(self, exception: Exception) -> bool:
        """Apply rescue_from strategy"""
        if not hasattr(self.execution.runnable.handler, 'rescue_from'):
            return False

        for strategy in self.execution.runnable.handler.rescue_from:
            if isinstance(exception, strategy.exceptions):
                logger.debug(f"Rescuing from {exception.__class__.__name__}")
                # Call rescue method if defined
                if hasattr(strategy, 'rescue'):
                    strategy.rescue(self.execution, exception)
                return True
        return False

    def should_retry(self, exception: Exception) -> bool:
        """Apply retry_on strategy"""
        if not hasattr(self.execution.runnable.handler, 'retry_on'):
            return False

        for strategy in self.execution.runnable.handler.retry_on:
            if isinstance(exception, strategy.exceptions) and self.failed_attempts < strategy.attempts:
                logger.debug(f"Retrying due to {exception.__class__.__name__}")
                self.failed_attempts += 1
                self.execution.retry(strategy, exception)
                return True
        return False

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
