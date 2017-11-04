import unittest
import across
from .utils import make_connection, MemoryChannel, Box


def nop(*args, **kwargs):
    pass


class ErrorTest(unittest.TestCase):
    def test_apply_after_close(self):
        conn = make_connection()
        conn.close()
        with self.assertRaises(across.ConnectionLost):
            conn.apply(nop)

    def test_connection_lost_during_call(self):
        channel = MemoryChannel()
        conn = across.Connection(channel)
        try:
            with self.assertRaises(across.ConnectionLost):
                conn.call(Box(channel.cancel))
            with self.assertRaises(across.ConnectionLost):
                conn.call(nop)
        finally:
            try:
                conn.close()
            except OSError:
                pass
