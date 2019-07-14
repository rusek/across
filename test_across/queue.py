import unittest
import threading
import gc
import platform
from queue import Empty

from across.utils import SimpleQueue


class Garbage:
    pass


def test_gc(margin, func, gc_func):
    def gc_callback(phase, info):
        if phase == 'start':
            gc_func()

    garbage = []
    garbage_append = garbage.append
    gc.collect()
    gc.callbacks.append(gc_callback)
    try:
        remaining = gc.get_threshold()[0] - gc.get_count()[0]
        for _ in range(remaining - margin):
            garbage_append(Garbage())
        func()
    finally:
        gc.callbacks.pop()


def iter_margins():
    return reversed(range(16))


class SimpleQueueTest(unittest.TestCase):
    def test_fifo_order(self):
        queue = SimpleQueue()
        for i in range(3):
            queue.put(i)
        for i in range(3):
            self.assertEqual(queue.get(), i)

    def test_get_with_timeout(self):
        queue = SimpleQueue()
        with self.assertRaises(Empty):
            queue.get(0.01)
        queue.put('x')
        self.assertEqual(queue.get(0.01), 'x')
        with self.assertRaises(Empty):
            queue.get(0.01)

    def test_multithread(self):
        queue = SimpleQueue()
        elems = []
        num_producers = 20
        num_elems_per_producer = 100

        def producer_loop(producer_id):
            for i in range(num_elems_per_producer):
                queue.put(producer_id * num_elems_per_producer + i)

        producer_threads = [threading.Thread(target=producer_loop, args=(i,)) for i in range(num_producers)]
        for thread in producer_threads:
            thread.start()

        for _ in range(num_producers * num_elems_per_producer):
            elems.append(queue.get())

        for thread in producer_threads:
            thread.join()

        self.assertEqual(sorted(elems), list(range(num_producers * num_elems_per_producer)))

    def test_gc_during_queue_put(self):
        for margin in iter_margins():
            queue = SimpleQueue()
            # Uncomment to see that Queue is not GC-safe (test hangs)
            # from queue import Queue
            # queue = Queue()
            queue_put = queue.put
            test_gc(margin, lambda: queue_put(None), lambda: queue_put(None))

    def test_gc_during_queue_get(self):
        for margin in iter_margins():
            queue = SimpleQueue()
            # Uncomment to see that Queue is not GC-safe (test hangs)
            # from queue import Queue
            # queue = Queue()
            queue_put = queue.put
            queue_get = queue.get
            queue_put(None)
            test_gc(margin, lambda: queue_get(), lambda: queue_put(None))

    def test_cpython(self):
        # across._SimpleQueue was designed to work with CPython - for other Python implementation, its code
        # should be carefully reviewed. The purpose of this test is to detect that someone is trying to use across
        # with other Python implementation, and warn that across._SimpleQueue requires special attention
        self.assertEqual(platform.python_implementation(), 'CPython')
