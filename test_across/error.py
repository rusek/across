import unittest
import across
import across.channels
import struct
import sys
from concurrent.futures import Future
from .utils import make_connection, MemoryChannel, Box


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

    def send(self, data):
        self.send_future.result()
        return len(data)

    def recv(self, size):
        self.recv_future.result()
        return b''

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


class ProtocolErrorChannel(across.channels.Channel):
    def __init__(self, data):
        self.__data = data

    def recv(self, size):
        data, self.__data = self.__data[:size], self.__data[size:]
        return data

    def send(self, data):
        return len(data)


class ProtocolErrorTest(unittest.TestCase):
    def __simulate_error(self, msg=None, frame=None, data=None, prepend_superblock=True, prepend_greeting=True):
        if msg is not None:
            frame = msg.as_bytes()
        if frame is not None:
            data = struct.pack('>I', len(frame)) + frame
        if prepend_superblock:
            if prepend_greeting:
                greeting_frame = across._get_greeting_frame(0)
                data = struct.pack('>I', len(greeting_frame)) + greeting_frame + data
            data = across._get_superblock() + data
        with across.Connection(ProtocolErrorChannel(data)) as conn:
            conn.wait()

    def test_incomplete_frame_size(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete frame size'):
            self.__simulate_error(data=b'')
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete frame size'):
            self.__simulate_error(data=b'\0\0\0')

    def test_incomplete_frame(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete frame'):
            self.__simulate_error(data=struct.pack('>I', 10))
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete frame'):
            self.__simulate_error(data=struct.pack('>I', 10) + b'abc')

    def test_invalid_apply_actor(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Actor not found: 404'):
            msg = across._Message()
            msg.put_uint(across._APPLY)
            msg.put_uint(404)
            self.__simulate_error(msg=msg)

    def test_invalid_result_actor(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Actor not found: 404'):
            msg = across._Message()
            msg.put_uint(across._RESULT)
            msg.put_uint(404)
            self.__simulate_error(msg=msg)

    def test_invalid_error_actor(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Actor not found: 404'):
            msg = across._Message()
            msg.put_uint(across._ERROR)
            msg.put_uint(404)
            self.__simulate_error(msg=msg)

    def test_invalid_operation_error_actor(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Actor not found: 404'):
            msg = across._Message()
            msg.put_uint(across._OPERATION_ERROR)
            msg.put_uint(404)
            self.__simulate_error(msg=msg)

    def test_incomplete_superblock(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete superblock'):
            self.__simulate_error(data=b'', prepend_superblock=False)
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete superblock'):
            self.__simulate_error(data=b'\0\0\0', prepend_superblock=False)

    def test_invalid_superblock_magic(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Invalid magic: .*'):
            superblock = b'x' * across._SUPERBLOCK_SIZE
            self.__simulate_error(data=superblock, prepend_superblock=False)

    def test_invalid_superblock_mode(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Invalid mode: 42'):
            superblock = struct.pack('>IB', across._MAGIC, 42) + b'\x00' * (across._SUPERBLOCK_SIZE - 5)
            self.__simulate_error(data=superblock, prepend_superblock=False)

    def test_incompatible_python_version(self):
        if sys.version_info[0] >= 3:
            python_version = (2, 7, 0)
        else:
            python_version = (3, 4, 0)
        msg = across._Message()
        msg.put_uint(across._GREETING)
        for num in python_version + across._version:
            msg.put_uint(num)
        with self.assertRaisesRegex(across.ProtocolError, 'Remote python .* is not compatible with local .*'):
            self.__simulate_error(msg=msg, prepend_greeting=False)

    def test_apply_in_greeting_state(self):
        with self.assertRaisesRegex(
            across.ProtocolError,
            'Invalid message in greeting state: {!r}'.format(across._APPLY)
        ):
            msg = across._Message()
            msg.put_uint(across._APPLY)
            self.__simulate_error(msg=msg, prepend_greeting=False)

    def test_greeting_in_ready_state(self):
        with self.assertRaisesRegex(
            across.ProtocolError,
            'Invalid message in ready state: {!r}'.format(across._GREETING)
        ):
            msg = across._Message()
            msg.put_uint(across._GREETING)
            self.__simulate_error(msg=msg)
