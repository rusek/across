import unittest
from concurrent.futures import Future

import across
from across.channels import Channel

from .utils import make_connection, make_channel_pair, MemoryChannel, Box, call_process


class StateTest(unittest.TestCase):
    def test_immediate_close(self):
        conn = make_connection()
        conn.close()

    def test_shutdown(self):
        call_process(_test_shutdown)


def _test_shutdown():
    for _ in range(3):
        conn = make_connection()
        conn.call(nop)


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
        conn = across.Connection(UnconnectableChannel())
        with self.assertRaises(ConnectError):
            conn.__enter__()
        with self.assertRaisesRegex(ValueError, 'Connection is closed'):
            conn.__enter__()

    def test_enter_after_cancel(self):
        # There are no writes to chan2, so handshake should never complete
        chan1, chan2 = make_channel_pair()
        conn = across.Connection(chan1)
        conn.cancel()
        with self.assertRaises(OSError):
            conn.__enter__()

    def test_exit_after_disconnect(self):
        chan = MemoryChannel()
        with self.assertRaises(ValueError):
            with across.Connection(chan) as conn:
                conn.call(Box(chan.cancel))

    def test_enter_after_remote_close(self):
        chan1, chan2 = make_channel_pair()
        conn1, conn2 = across.Connection(chan1), across.Connection(chan2)
        conn2.close()
        with conn1:
            pass


def nop(*args, **kwargs):
    pass


class FirstError(Exception):
    pass


class SecondError(Exception):
    pass


class FailingChannel(across.channels.Channel):
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
        with self.assertRaises(across.DisconnectError):
            conn.call(nop)

    def test_disconnect_during_call(self):
        channel = MemoryChannel()
        conn = across.Connection(channel)
        try:
            with self.assertRaises(across.DisconnectError):
                conn.call(Box(channel.cancel))
            with self.assertRaises(across.DisconnectError):
                conn.call(nop)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def test_call_after_cancel(self):
        conn = make_connection()
        conn.cancel()
        with self.assertRaises(across.DisconnectError):
            conn.call(nop)
        try:
            conn.close()
        except Exception:
            pass

    def test_close_after_cancel(self):
        conn = make_connection()
        conn.cancel()
        # TODO maybe close() shouldn't raise exception in this case?
        with self.assertRaises(OSError):
            conn.close()

    def test_connect_exception(self):
        chan = FailingChannel()
        chan.connect_future.set_exception(FirstError())
        with self.assertRaises(FirstError):
            with across.Connection(chan):
                pass

    def test_recv_exception_is_ignored_after_send_exception(self):
        chan = FailingChannel()
        chan.connect_future.set_result(None)
        chan.close_future.set_result(None)
        with self.assertRaises(FirstError):
            conn = across.Connection(chan)
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
            with across.Connection(chan) as conn:
                conn.wait()
