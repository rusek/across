import unittest
import threading
import time

from across.utils import Executor


class Counter(object):
    def __init__(self):
        self._lock = threading.Lock()
        self._value = 0

    def increment(self):
        with self._lock:
            self._value += 1

    def get(self):
        with self._lock:
            return self._value


class ExecutorTest(unittest.TestCase):
    def test_parallel_calls(self):
        num_calls = 5
        barrier = threading.Barrier(num_calls + 1)  # ' + 1' for current thread
        executor = Executor()
        for _ in range(num_calls):
            executor.submit(barrier.wait)
        barrier.wait()
        executor.close()

    def test_sequential_calls_with_delay(self):
        num_calls = 5
        counter = Counter()
        executor = Executor()
        for _ in range(num_calls):
            executor.submit(counter.increment)
            time.sleep(0.01)
        executor.close()
        self.assertEqual(counter.get(), 5)

    def test_close_blocks(self):
        def func():
            time.sleep(0.01)
            counter.increment()

        counter = Counter()
        executor = Executor()
        executor.submit(func)
        executor.close()
        self.assertEqual(counter.get(), 1)

    def test_idle_timeout(self):
        num_threads = threading.active_count()
        executor = Executor(idle_timeout=0.01)
        executor.submit(lambda: None)
        while threading.active_count() != num_threads:
            time.sleep(0.01)

    def test_submit_after_idle_timeout(self):
        counter = Counter()
        executor = Executor(idle_timeout=0.01)
        executor.submit(counter.increment)
        time.sleep(0.02)
        executor.submit(counter.increment)
        executor.close()
        self.assertEqual(counter.get(), 2)

    def test_submit_after_close(self):
        executor = Executor()
        executor.close()
        self.assertRaises(ValueError, executor.submit, lambda: None)
