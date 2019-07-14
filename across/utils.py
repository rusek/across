"""
Various helpers. For internal use only.
"""

import threading
import collections
import traceback
import sys


class Future:
    def __init__(self):
        self._done = threading.Event()
        self._result = None

    def result(self):
        self._done.wait()
        return self._result

    def set_result(self, result):
        self._result = result
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
    except BaseException:
        pass
