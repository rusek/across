import unittest
import across
import struct
import sys
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


class ProtocolErrorChannel(across.Channel):
    def __init__(self, data):
        self.__data = data

    def recv(self, size):
        data, self.__data = self.__data[:size], self.__data[size:]
        return data

    def send(self, data):
        pass

    def cancel(self):
        pass

    def close(self):
        pass


class ProtocolErrorTest(unittest.TestCase):
    def __simulate_error(self, data, prepend_greeting=True):
        if prepend_greeting:
            data = across._get_greeting_frame() + data
        with across.Connection(ProtocolErrorChannel(data)) as conn:
            conn.wait()

    def test_incomplete_msg_size(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete message size'):
            self.__simulate_error(b'\0\0\0')

    def test_empty_msg(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Empty message'):
            self.__simulate_error(struct.pack('>I', 0))

    def test_incomplete_msg(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete message'):
            self.__simulate_error(struct.pack('>I', 10))
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete message'):
            self.__simulate_error(struct.pack('>I', 10) + b'abc')

    def test_invalid_msg_type(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Invalid message type: 99'):
            self.__simulate_error(struct.pack('>IB', 1, 99))

    def test_incomplete_apply_msg(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete apply message'):
            self.__simulate_error(struct.pack('>IB', 1 + 7, across._APPLY) + b'\0' * 7)

    def test_invalid_apply_actor(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Actor not found: 404'):
            self.__simulate_error(struct.pack('>IBQ', 1 + 8, across._APPLY, 404))

    def test_incomplete_result_msg(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete result message'):
            self.__simulate_error(struct.pack('>IB', 1 + 7, across._RESULT) + b'\0' * 7)

    def test_invalid_result_actor(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Actor not found: 404'):
            self.__simulate_error(struct.pack('>IBQ', 1 + 8, across._RESULT, 404))

    def test_incomplete_error_msg(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete error message'):
            self.__simulate_error(struct.pack('>IB', 1 + 7, across._ERROR) + b'\0' * 7)

    def test_invalid_error_actor(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Actor not found: 404'):
            self.__simulate_error(struct.pack('>IBQ', 1 + 8, across._ERROR, 404))

    def test_incomplete_greeting_msg(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Incomplete greeting message'):
            self.__simulate_error(struct.pack('>IB', 1 + 5, across._GREETING) + b'\0' * 5, prepend_greeting=False)

    def test_incompatible_python_version(self):
        if sys.version_info[0] >= 3:
            python_version = (2, 7, 0)
        else:
            python_version = (3, 4, 0)
        frame = struct.pack('>IBBBBBBB', 1 + 6, across._GREETING, *python_version + across._version)
        with self.assertRaisesRegex(across.ProtocolError, 'Remote python .* is not compatible with local .*'):
            self.__simulate_error(frame, prepend_greeting=False)

    def test_apply_in_greeting_state(self):
        with self.assertRaisesRegex(
            across.ProtocolError,
            'Unexpected message in greeting state: %r' % (across._APPLY, )
        ):
            self.__simulate_error(struct.pack('>IBQ', 1 + 8, across._APPLY, 1), prepend_greeting=False)

    def test_greeting_in_ready_state(self):
        with self.assertRaisesRegex(
            across.ProtocolError,
            'Unexpected message in ready state: %r' % (across._GREETING, )
        ):
            self.__simulate_error(struct.pack('>IB', 1 + 6, across._GREETING) + b'\0' * 6)
