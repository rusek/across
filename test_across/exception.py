import unittest
import pickle
import traceback
from .utils import Box, make_connection


class TestError(Exception):
    pass


class TestError1(TestError):
    pass


class TestError2(TestError):
    pass


class TestError3(TestError):
    pass


def tb_to_list(tb):
    result = []
    while tb is not None:
        result.append(tb)
        tb = tb.tb_next
    return result


class ExceptionTest(unittest.TestCase):
    def _capture_exc(self, func, *args):
        try:
            func(*args)
        except TestError as exc:
            return exc
        self.fail('TestError not raised')

    def _capture_excs(self, func):
        ref_exc = self._capture_exc(func)  # reference exception
        with make_connection() as conn:
            exc = self._capture_exc(conn.call, Box(func))
        return ref_exc, exc

    def test_context(self):
        def func():
            try:
                raise TestError1('msg1')
            except TestError1:
                raise TestError2('msg2')

        for exc in self._capture_excs(func):
            self.assertIsInstance(exc, TestError2)
            self.assertEqual(str(exc), 'msg2')
            self.assertIsInstance(exc.__context__, TestError1)
            self.assertEqual(str(exc.__context__), 'msg1')
            self.assertIsNone(exc.__context__.__context__)
            self.assertFalse(exc.__context__.__suppress_context__)
            self.assertIsNone(exc.__context__.__cause__)
            self.assertFalse(exc.__suppress_context__)
            self.assertIsNone(exc.__cause__)

    def test_nested_context(self):
        def func():
            try:
                try:
                    raise TestError1
                except TestError1:
                    raise TestError2
            except TestError2:
                raise TestError3

        for exc in self._capture_excs(func):
            self.assertIsInstance(exc, TestError3)
            self.assertIsInstance(exc.__context__, TestError2)
            self.assertIsInstance(exc.__context__.__context__, TestError1)
            self.assertIsNone(exc.__context__.__context__.__context__)

    # this test is meant to detect changes in the way exceptions are pickled in Python; across has custom
    # exception pickling strategy, because in the current Python versions __context__ is lost during
    # serialization
    def test_lost_context(self):
        def func():
            try:
                raise TestError1
            except TestError1:
                raise TestError2

        exc = self._capture_exc(func)
        exc = pickle.loads(pickle.dumps(exc))
        self.assertIsNone(exc.__context__)

    def test_cause(self):
        def func():
            try:
                raise TestError1('msg1')
            except TestError as error:
                cause = error
            raise TestError2('msg2') from cause

        for exc in self._capture_excs(func):
            self.assertIsInstance(exc, TestError2)
            self.assertEqual(str(exc), 'msg2')
            self.assertIsNone(exc.__context__)
            self.assertTrue(exc.__suppress_context__)
            self.assertIsInstance(exc.__cause__, TestError1)
            self.assertEqual(str(exc.__cause__), 'msg1')
            self.assertIsNone(exc.__cause__.__context__)
            self.assertFalse(exc.__cause__.__suppress_context__)
            self.assertIsNone(exc.__cause__.__cause__)

    def test_nested_cause(self):
        def func():
            try:
                raise TestError1
            except TestError1 as error:
                cause = error
            try:
                raise TestError2 from cause
            except TestError2 as error:
                cause = error
            raise TestError3 from cause

        for exc in self._capture_excs(func):
            self.assertIsInstance(exc, TestError3)
            self.assertIsInstance(exc.__cause__, TestError2)
            self.assertIsInstance(exc.__cause__.__cause__, TestError1)
            self.assertIsNone(exc.__cause__.__cause__.__cause__)

    # this test is meant to detect changes in the way exceptions are pickled in Python; across has custom
    # exception pickling strategy, because in the current Python versions __cause__ is lost during
    # serialization
    def test_lost_cause(self):
        def func():
            try:
                raise TestError1
            except TestError1 as error:
                cause = error
            raise TestError2 from cause

        exc = self._capture_exc(func)
        exc = pickle.loads(pickle.dumps(exc))
        self.assertIsNone(exc.__cause__)
        self.assertFalse(exc.__suppress_context__)

    def test_none_cause(self):
        def func():
            try:
                raise TestError1
            except TestError1:
                raise TestError2 from None

        for exc in self._capture_excs(func):
            self.assertIsInstance(exc, TestError2)
            self.assertIsInstance(exc.__context__, TestError1)
            self.assertTrue(exc.__suppress_context__)
            self.assertIsNone(exc.__cause__)

    def test_context_and_cause(self):
        def func():
            try:
                raise TestError1
            except TestError1:
                raise TestError2 from TestError3()

        for exc in self._capture_excs(func):
            self.assertIsInstance(exc, TestError2)
            self.assertIsInstance(exc.__context__, TestError1)
            self.assertTrue(exc.__suppress_context__)
            self.assertIsInstance(exc.__cause__, TestError3)

    def test_traceback(self):
        def func1():
            raise TestError1

        def func2():
            func1()

        ref_exc, exc = self._capture_excs(func2)
        # .tb_next is for skipping _capture_exc frame
        ref_tb_lines = ''.join(traceback.format_tb(ref_exc.__traceback__.tb_next)).splitlines(True)
        tb_lines = ''.join(traceback.format_tb(exc.__traceback__)).splitlines(True)
        self.assertEqual(ref_tb_lines, tb_lines[-len(ref_tb_lines):])

    def test_traceback_with_nested_connections(self):
        def func_over_conn(depth):
            if depth > 0:
                with make_connection() as conn:
                    conn.call(Box(func_over_conn), depth - 1)
            else:
                raise TestError

        exc = self._capture_exc(func_over_conn, 3)
        formatted_tb = ''.join(traceback.format_tb(exc.__traceback__))

        self.assertEqual(
            formatted_tb.count('conn.call(Box(func_over_conn), depth - 1)'),
            3,
            formatted_tb,
        )
