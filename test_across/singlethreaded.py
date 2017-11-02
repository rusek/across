import unittest
import across
from .memchan import MemoryChannel


def make_connection():
    return across.Connection(MemoryChannel())


magic = 'abracadabra'


def get_magic():
    return magic


def subtract(left, right):
    return left - right


def get_args(*args, **kwargs):
    return args, kwargs


class SingleThreadedTest(unittest.TestCase):
    def test_immediate_close(self):
        conn = make_connection()
        conn.close()

    def test_apply_without_args(self):
        with make_connection() as conn:
            self.assertEqual(conn.apply(get_magic), magic)
            self.assertEqual(conn.apply(get_magic, ()), magic)
            self.assertEqual(conn.apply(get_magic, (), {}), magic)
            self.assertEqual(conn.apply(get_magic, None), magic)
            self.assertEqual(conn.apply(get_magic, None, None), magic)
            self.assertEqual(conn.apply(get_magic, None, {}), magic)
            self.assertEqual(conn.apply(get_magic, (), None), magic)

    def test_apply_with_args(self):
        with make_connection() as conn:
            self.assertEqual(conn.apply(subtract, (3, 2)), 1)
            self.assertEqual(conn.apply(subtract, (3, ), dict(right=2)), 1)
            self.assertEqual(conn.apply(subtract, (), dict(left=3, right=2)), 1)

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
