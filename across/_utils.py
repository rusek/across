"""
Various helpers. For internal use only.
"""

import collections
import traceback
import sys
import queue
import logging
import threading

from ._importer import logger, get_debug_level, set_debug_level


# Export utilities implemented in importer module
logger = logger
get_debug_level = get_debug_level
set_debug_level = set_debug_level


class _AtomicCounter:
    def __init__(self, start=0):
        self._value = start
        self._lock = threading.Lock()

    def __next__(self):
        with self._lock:
            value = self._value
            self._value += 1
            return value

    def __iter__(self):
        return self


atomic_count = _AtomicCounter


class IdentityAdapter(logging.LoggerAdapter):
    def __init__(self, logger, identity):
        super().__init__(logger, {})
        self._prefix = '[{}] '.format(identity)

    def process(self, msg, kwargs):
        return self._prefix + msg, kwargs


# Lightweight version of concurrent.futures.Future
class Future:
    def __init__(self):
        self._done = threading.Event()
        self._result = None
        self._exc = None

    def result(self):
        self._done.wait()
        if self._exc is not None:
            raise self._exc
        return self._result

    def set_result(self, result):
        self._result = result
        self._done.set()

    def set_exception(self, exc):
        self._exc = exc
        self._done.set()


# Runs functions in worker threads.
class Executor:
    def __init__(self, idle_timeout=10.0):
        self._lock = threading.Lock()
        self._tasks = collections.deque()
        self._idle_condition = threading.Condition(self._lock)
        self._num_idle_threads = 0
        self._num_threads = 0
        self._idle_timeout = idle_timeout
        self._joinable_thread = None
        self._closing = False
        self._close_condition = threading.Condition(self._lock)

    def submit(self, func, *args):
        with self._lock:
            if self._closing:
                raise ValueError('Executor is closed')
            if self._num_idle_threads > 0:
                self._tasks.append((func, args))
                self._num_idle_threads -= 1
                self._idle_condition.notify()
            else:
                thread = threading.Thread(target=self._thread_func, args=(func, args), daemon=True)
                thread.start()
                self._num_threads += 1

    def _thread_func(self, func, args):
        while True:
            try:
                func(*args)
            # All exceptions from threading.Thread.run() method are ignored, here we emulate that behavior.
            except BaseException:
                ignore_exception_at(func)

            with self._lock:
                if self._closing:
                    break
                self._num_idle_threads += 1
                while not self._tasks and not self._closing:
                    if not self._idle_condition.wait(self._idle_timeout) and self._num_idle_threads > 0:
                        self._num_idle_threads -= 1
                        break
                if not self._tasks:
                    break
                func, args = self._tasks.popleft()

        with self._lock:
            self._num_threads -= 1
            if self._closing and self._num_threads == 0:
                self._close_condition.notify_all()
            if self._joinable_thread:
                self._joinable_thread.join()
            self._joinable_thread = threading.current_thread()

    def close(self):
        with self._lock:
            self._closing = True
            self._num_idle_threads = 0
            self._idle_condition.notify_all()
            while self._num_threads > 0:
                self._close_condition.wait()
            if self._joinable_thread:
                self._joinable_thread.join()
                self._joinable_thread = None


# based on PyErr_WriteUnraisable
def ignore_exception_at(obj):
    try:
        print('Exception ignored in: {!r}'.format(obj), file=sys.stderr)
        traceback.print_exc()
    except OSError:
        pass


# Variant of traceback.format_exception_only that really formats only the exception. For SyntaxError,
# traceback.format_exception_only outputs also some additional lines that pretend to be a part of the traceback.
def format_exception_only(error):
    if isinstance(error, SyntaxError):
        return 'SyntaxError: {}'.format(error)
    return ''.join(traceback.format_exception_only(type(error), error)).strip()


# Synchronized queue implementation that can be safely used from within __del__ methods, as opposed to
# queue.Queue (https://bugs.python.org/issue14976). In Python 3.7, queue.SimpleQueue can be used instead.
#
# This class was written with CPython in mind, and may not work correctly with other Python implementations.
class SimpleQueue:
    def __init__(self):
        self.__lock = threading.Lock()
        self.__waiter = threading.Lock()
        self.__items = collections.deque()

        # Create bound method objects for later use in get()/put() - creating such objects may trigger GC,
        # which must not happen inside get()/put(). These methods are all written in C, and are expected not to
        # allocate GC memory internally (e.g. with PyObject_GC_New).
        self.__lock_acquire = self.__lock.acquire
        self.__lock_release = self.__lock.release
        self.__waiter_acquire = self.__waiter.acquire
        self.__waiter_release = self.__waiter.release
        self.__items_popleft = self.__items.popleft
        self.__items_append = self.__items.append

        self.__waiter_acquire()

    # Wait until a queue becomes non-empty, and retrieve a single element from the queue. This function may be
    # called only from a single thread at a time.
    def get(self, timeout=None):
        if timeout is None:
            self.__waiter_acquire()
        elif not self.__waiter_acquire(timeout=timeout):
            raise queue.Empty
        self.__lock_acquire()
        item = self.__items_popleft()
        if self.__items:
            self.__waiter_release()
        self.__lock_release()
        return item

    # Insert a single element at the end of the queue. This method may be safely called from multiple threads,
    # and from __del__ methods.
    def put(self, item):
        self.__lock_acquire()
        if not self.__items:
            self.__waiter_release()
        self.__items_append(item)
        self.__lock_release()
