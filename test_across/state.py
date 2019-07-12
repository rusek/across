import unittest
import across
from across.channels import Channel
from .utils import make_connection, make_channel_pair, MemoryChannel, Box


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
