import unittest
from concurrent.futures import Future

from across import Connection, DisconnectError, Options
from across._channels import Channel

from .utils import make_connection, make_channel_pair, ConnectionChannel, Box, call_process_with_stderr


class StateTest(unittest.TestCase):
    def test_immediate_close(self):
        conn = make_connection()
        conn.close()

    def test_shutdown(self):
        # across._shutdown() calls Connection.cancel(), which causes various exceptions to be printed
        # to stderr. Let's ignore them.
        call_process_with_stderr(self._run_shutdown_test)

    @staticmethod
    def _run_shutdown_test():
        for _ in range(3):
            conn = make_connection()
            conn.call(nop)

    def test_shutdown_with_unresponsive_connections(self):
        call_process_with_stderr(self._run_shutdown_with_unresponsive_connections_test)

    @staticmethod
    def _run_shutdown_with_unresponsive_connections_test():
        Connection(make_channel_pair()[0], options=Options(timeout=1e100))


class ConnectError(Exception):
    pass


class UnconnectableChannel(Channel):
    def connect(self):
        raise ConnectError

    def send(self, buffer):
        raise AssertionError

    def recv_into(self, buffer):
        raise AssertionError


class ContextManagerTest(unittest.TestCase):
    def test_enter_after_close(self):
        conn = make_connection()
        conn.close()
        with self.assertRaisesRegex(ValueError, 'Connection is closed'):
            conn.__enter__()

    def test_enter_when_connect_fails(self):
        conn = Connection(UnconnectableChannel())
        with self.assertRaises(ConnectError):
            conn.__enter__()
        with self.assertRaisesRegex(ValueError, 'Connection is closed'):
            conn.__enter__()

    def test_enter_after_cancel(self):
        # There are no writes to chan2, so handshake should never complete
        chan1, chan2 = make_channel_pair()
        conn = Connection(chan1)
        conn.cancel()
        with self.assertRaises(ValueError):
            conn.__enter__()

    def test_exit_after_disconnect(self):
        chan = ConnectionChannel()
        with self.assertRaises(ValueError):
            with Connection(chan) as conn:
                conn.call(Box(chan.cancel))

    def test_enter_after_remote_close(self):
        chan1, chan2 = make_channel_pair()
        conn1, conn2 = Connection(chan1), Connection(chan2)
        conn2.close()
        with conn1:
            pass


def nop(*args, **kwargs):
    pass


class FirstError(Exception):
    pass


class SecondError(Exception):
    pass


class FailingChannel(Channel):
    def __init__(self):
        self.connect_future = Future()
        self.send_future = Future()
        self.recv_future = Future()
        self.close_future = Future()

    def connect(self):
        self.connect_future.result()

    def send(self, buffer):
        self.send_future.result()
        return len(buffer)

    def recv_into(self, buffer):
        self.recv_future.result()
        return 0

    def close(self):
        self.close_future.result()


class DisconnectErrorTest(unittest.TestCase):
    def test_call_after_close(self):
        conn = make_connection()
        conn.close()
        with self.assertRaisesRegex(DisconnectError, 'Connection is closed'):
            conn.call(nop)

    def test_communication_error_during_call(self):
        chan = ConnectionChannel()
        conn = Connection(chan)
        try:
            with self.assertRaisesRegex(DisconnectError, 'Connection was aborted due to .*'):
                conn.call(Box(chan.cancel))  # simulate communication error with channel.cancel
            with self.assertRaisesRegex(DisconnectError, 'Connection was aborted due to .*'):
                conn.call(nop)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def test_remote_close_during_call(self):
        chan, rchan = make_channel_pair()
        conn = Connection(chan)
        Connection(rchan).close()
        with self.assertRaisesRegex(DisconnectError, 'Connection was remotely closed'):
            conn.call(nop)
        conn.close()

    def test_call_after_cancel(self):
        conn = make_connection()
        conn.cancel()
        with self.assertRaisesRegex(DisconnectError, 'Connection was cancelled'):
            conn.call(nop)
        try:
            conn.close()
        except Exception:
            pass

    def test_close_after_cancel(self):
        conn = make_connection()
        conn.cancel()
        conn.close()

    def test_connect_exception(self):
        chan = FailingChannel()
        chan.connect_future.set_exception(FirstError())
        chan.close_future.set_result(None)
        with self.assertRaises(FirstError):
            with Connection(chan):
                pass

    def test_recv_exception_is_ignored_after_send_exception(self):
        chan = FailingChannel()
        chan.connect_future.set_result(None)
        chan.close_future.set_result(None)
        with self.assertRaises(FirstError):
            conn = Connection(chan)
            chan.send_future.set_exception(FirstError())
            conn.wait()
            chan.recv_future.set_exception(SecondError())
            conn.close()

    def test_channel_close_exception_is_ignored_after_previous_exception(self):
        chan = FailingChannel()
        chan.connect_future.set_result(None)
        chan.send_future.set_result(None)
        chan.recv_future.set_exception(FirstError())
        chan.close_future.set_exception(SecondError())
        with self.assertRaises(FirstError):
            with Connection(chan) as conn:
                conn.wait()
