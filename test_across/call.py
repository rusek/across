import unittest
import threading

import across

from .utils import make_connection, Box, par


magic = 'abracadabra'


def get_magic():
    return magic


def subtract(left, right):
    return left - right


def get_args(*args, **kwargs):
    return args, kwargs


class CallTest(unittest.TestCase):
    def test_call(self):
        with make_connection() as conn:
            self.assertEqual(conn.call(get_magic), magic)
            self.assertEqual(conn.call(subtract, 3, 2), 1)
            self.assertEqual(conn.call(subtract, 3, right=2), 1)
            self.assertEqual(conn.call(subtract, left=3, right=2), 1)

    def test_call_special_name_handling(self):
        with make_connection() as conn:
            self.assertEqual(conn.call(get_args, self=1), ((), dict(self=1)))
            self.assertEqual(conn.call(get_args, func=1), ((), dict(func=1)))
            with self.assertRaises(TypeError):  # sanity check
                (lambda a: None)()
            with self.assertRaises(TypeError):
                conn.call()
            with self.assertRaises(TypeError):
                across.Connection.call()

    def test_nested_call(self):
        @Box
        def factorial(n):
            return 1 if n == 0 else n * across.get_connection().call(factorial, n - 1)

        with make_connection() as conn:
            self.assertEqual(conn.call(factorial, 5), 120)

    def test_parallel_call(self):
        with make_connection() as conn:
            barrier = threading.Barrier(3)
            par(*[lambda: conn.call(Box(barrier.wait))] * 3)
