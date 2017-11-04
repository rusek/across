import unittest
import across
from .utils import make_connection, Box


def nop(*args, **kwargs):
    pass


class PickleObserver(object):
    def __init__(self, on_pickle=None, on_unpickle=None):
        self.__on_pickle = Box(on_pickle)
        self.__on_unpickle = Box(on_unpickle)

    def __getstate__(self):
        if self.__on_pickle.value is not None:
            self.__on_pickle.value()
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__ = state
        if self.__on_unpickle.value is not None:
            self.__on_unpickle.value()


class GetConnectionTest(unittest.TestCase):
    def test_get_connection_during_pickling(self):
        with make_connection() as conn:
            def on_pickle():
                self.assertIs(across.get_connection(), conn)

            with self.assertRaises(RuntimeError):
                across.get_connection()
            conn.call(nop, PickleObserver(on_pickle=on_pickle))
            with self.assertRaises(RuntimeError):
                across.get_connection()

    def test_get_connection_during_unpickling(self):
        with make_connection() as conn:
            def on_unpickle():
                remote_conn = across.get_connection()
                self.assertIsInstance(remote_conn, across.Connection)
                self.assertIsNot(remote_conn, conn)

            with self.assertRaises(RuntimeError):
                across.get_connection()
            conn.call(nop, PickleObserver(on_unpickle=on_unpickle))
            with self.assertRaises(RuntimeError):
                across.get_connection()

    def test_get_connection_during_invocation(self):
        with make_connection() as conn:
            def invoke():
                remote_conn = across.get_connection()
                self.assertIsInstance(remote_conn, across.Connection)
                self.assertIsNot(remote_conn, conn)

            with self.assertRaises(RuntimeError):
                across.get_connection()
            conn.call(Box(invoke))
            with self.assertRaises(RuntimeError):
                across.get_connection()

    def test_get_connection_during_nested_invocation(self):
        with make_connection() as conn:
            def invoke():
                remote_conn = across.get_connection()
                self.assertIsInstance(remote_conn, across.Connection)
                self.assertIsNot(remote_conn, conn)
                remote_conn.call(Box(invoke_nested))
                self.assertIs(across.get_connection(), remote_conn)

            def invoke_nested():
                self.assertIs(across.get_connection(), conn)

            with self.assertRaises(RuntimeError):
                across.get_connection()
            conn.call(Box(invoke))
            with self.assertRaises(RuntimeError):
                across.get_connection()
