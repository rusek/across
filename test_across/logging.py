import unittest
import logging

from across.logging import AcrossHandler
import across

from .utils import make_connection, Box, StderrCollector


class _ListHandler(logging.Handler):
    def __init__(self):
        super(_ListHandler, self).__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class _RemoteSideFilter(logging.Filter):
    def __init__(self, negate=False):
        super(_RemoteSideFilter, self).__init__()
        self._negate = negate

    def filter(self, record):
        try:
            conn = across.get_connection()
        except RuntimeError:
            conn = None
        return (conn == _remote_conn.value) != self._negate


def _nop(*args, **kwargs):
    pass


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)
_logger.propagate = False

_handler = _ListHandler()
_handler.setLevel(logging.INFO)
_handler.addFilter(_RemoteSideFilter(negate=True))
_logger.addHandler(_handler)

_remote_handler = Box()
_remote_conn = Box()


def _setup_remote_logging():
    _remote_conn.value = across.get_connection()
    handler = AcrossHandler()
    handler.addFilter(_RemoteSideFilter())
    handler.setLevel(logging.INFO)
    _logger.addHandler(handler)
    _remote_handler.value = handler


def _log(*args, **kwargs):
    _logger.info(*args, **kwargs)


class _Unpickleable(object):
    def __init__(self, value):
        self.__value = value

    def __reduce__(self):
        raise AssertionError

    def __str__(self):
        return str(self.__value)

    def __int__(self):
        return int(self.__value)


class LoggingTest(unittest.TestCase):
    def tearDown(self):
        if _remote_handler.value:
            _logger.removeHandler(_remote_handler.value)
            _remote_handler.value = None
        _handler.records.clear()
        logging.raiseExceptions = True  # expected default value

    def test_msg(self):
        msg = 'hello'
        with make_connection() as conn:
            conn.call(_setup_remote_logging)
            conn.call(_log, msg)
        self.assertEqual(len(_handler.records), 1)
        self.assertEqual(_handler.records[0].getMessage(), msg)

    def test_msg_with_args(self):
        with make_connection() as conn:
            conn.call(_setup_remote_logging)
            conn.call(_log, 'msg, x=%s, 1=%d', 'x', 1)
        self.assertEqual(len(_handler.records), 1)
        self.assertEqual(_handler.records[0].getMessage(), 'msg, x=x, 1=1')

    def test_msg_with_unpickleable_args(self):
        def log_unpickleable():
            _log('msg, x=%s, 1=%d', _Unpickleable('x'), _Unpickleable(1))

        with make_connection() as conn:
            conn.call(_setup_remote_logging)
            conn.call(Box(log_unpickleable))
        self.assertEqual(len(_handler.records), 1)
        self.assertEqual(_handler.records[0].getMessage(), 'msg, x=x, 1=1')

    def _make_record(self, msg):
        return _logger.makeRecord(_logger.name, logging.INFO, 'fn', 1, msg, (), None)

    def test_formatter_garbage_is_dropped(self):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        msg = 'hello'
        orig_fields = set()

        def log_remotely():
            record = self._make_record(msg)
            orig_fields.update(record.__dict__.keys())
            formatter.format(record)
            _logger.handle(record)

        with make_connection() as conn:
            conn.call(_setup_remote_logging)
            conn.call(Box(log_remotely))

        self.assertEqual(len(_handler.records), 1)
        self.assertGreater(len(orig_fields), 0)  # sanity check
        final_fields = set(_handler.records[0].__dict__.keys())
        self.assertEqual(orig_fields - final_fields, set())
        self.assertEqual(final_fields - orig_fields, set())

    def test_disconnected_connection(self):
        with make_connection() as conn:
            conn.call(_nop)
        handler = AcrossHandler(conn)
        with StderrCollector() as stderr:
            handler.handle(self._make_record('whatever'))
        self.assertEqual(stderr.getvalue(), '')

    def test_unexpected_error_handling(self):
        with make_connection() as conn:
            record = self._make_record('whatever')
            record.unpickleable = _Unpickleable(None)
            handler = AcrossHandler(conn)

            for raise_exceptions in (True, False):
                logging.raiseExceptions = raise_exceptions
                with StderrCollector() as stderr:
                    handler.handle(record)
                if raise_exceptions:
                    self.assertIn(across.OperationError.__name__, stderr.getvalue())
                else:
                    self.assertEqual(stderr.getvalue(), '')

            logging.raiseExceptions = True

    def test_exc_info(self):
        log_msg = '[some log message]'
        exc_msg = '[some exception message]'

        def exc_raising_func():
            raise ValueError(exc_msg)

        def log_exc():
            try:
                exc_raising_func()
            except ValueError:
                _log(log_msg, exc_info=True)

        with make_connection() as conn:
            conn.call(_setup_remote_logging)
            conn.call(Box(log_exc))

        self.assertEqual(len(_handler.records), 1)
        text = logging.Formatter().format(_handler.records[0])
        self.assertIn(log_msg, text)
        self.assertIn(exc_msg, text)
        # check traceback is included
        self.assertIn(exc_raising_func.__name__, text)

    def test_stack_info(self):
        log_msg = '[some log message]'

        def stack_logging_func():
            _log(log_msg, stack_info=True)

        with make_connection() as conn:
            conn.call(_setup_remote_logging)
            conn.call(Box(stack_logging_func))

        self.assertEqual(len(_handler.records), 1)
        text = logging.Formatter().format(_handler.records[0])
        self.assertIn(log_msg, text)
        self.assertIn(stack_logging_func.__name__, text)
