import unittest
import across
import threading
from .utils import make_connection, Box


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

    def test_nested_call_reuses_threads(self):
        remote_thread = Box()
        local_thread = threading.current_thread()
        with make_connection() as conn:
            @Box
            def call_local_loop(n):
                self.assertIs(local_thread, threading.current_thread())
                if n:
                    conn.call(call_remote_loop, n - 1)

            @Box
            def call_remote_loop(n):
                self.assertIs(remote_thread.value, threading.current_thread())
                across.get_connection().call(call_local_loop, n)

            @Box
            def call_remote():
                remote_thread.value = threading.current_thread()
                across.get_connection().call(call_local_loop, 5)

            conn.call(call_remote)

    def test_exception(self):
        def func():
            raise ValueError
        with make_connection() as conn:
            with self.assertRaises(ValueError):
                conn.call(Box(func))


class BrokenPickle(object):
    def __reduce__(self):
        raise TypeError('%s is not pickleable' % (self.__class__.__name__, ))


class BrokenPickleError(BrokenPickle, Exception):
    pass


def _reduce_broken_unpickle(cls):
    raise TypeError('%s is not unpickleable' % (cls.__name__, ))


class BrokenUnpickle(object):
    def __reduce__(self):
        return _reduce_broken_unpickle, (self.__class__, )


class BrokenUnpickleError(BrokenUnpickle, Exception):
    pass


class PicklingExceptionTest(unittest.TestCase):
    def test_pickle_error_in_call_func(self):
        with make_connection() as conn:
            with self.assertRaises(across.OperationError):
                conn.call(lambda: None)
            self.assertEqual(conn.call(get_magic), magic)

    def test_pickle_error_in_call_argument(self):
        with make_connection() as conn:
            with self.assertRaises(across.OperationError):
                conn.call(get_args, BrokenPickle())
            self.assertEqual(conn.call(get_magic), magic)

    def test_pickle_error_in_call_result(self):
        def func():
            return BrokenPickle()

        with make_connection() as conn:
            with self.assertRaises(across.OperationError):
                conn.call(Box(func))
            self.assertEqual(conn.call(get_magic), magic)

    def test_pickle_error_call_error(self):
        def func():
            raise BrokenPickleError()

        with make_connection() as conn:
            with self.assertRaises(across.OperationError):
                conn.call(Box(func))
            self.assertEqual(conn.call(get_magic), magic)

    def test_unpickle_error_in_call(self):
        with make_connection() as conn:
            with self.assertRaises(across.OperationError):
                conn.call(BrokenUnpickle())
            self.assertEqual(conn.call(get_magic), magic)

    def test_unpickle_error_in_call_result(self):
        def func():
            return BrokenUnpickle()

        with make_connection() as conn:
            with self.assertRaises(across.OperationError):
                conn.call(Box(func))
            self.assertEqual(conn.call(get_magic), magic)

    def test_unpickle_error_in_call_error(self):
        def func():
            raise BrokenUnpickleError()

        with make_connection() as conn:
            with self.assertRaises(across.OperationError):
                conn.call(Box(func))
            self.assertEqual(conn.call(get_magic), magic)
