import unittest
import threading
from .utils import make_connection, par, Box


class MultiThreadedTest(unittest.TestCase):
    def test_parallel_call(self):
        with make_connection() as conn:
            barrier = threading.Barrier(3)
            par(*[lambda: conn.call(Box(barrier.wait))] * 3)
