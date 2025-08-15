from .quebec import * # NOQA
import logging
import time
import queue
import threading
from functools import wraps
from typing import Callable, List, Tuple, Type, Any, Optional
import dataclasses
from .logger import job_id_var

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
    pass


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

                # Inject job_id into the context before execution, clean up after execution.
                token = job_id_var.set(str(self.execution.jid))
                self.execution.perform()
                logger.debug(self.execution.metric)
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
