import unittest
import struct
import sys

import across
import across.channels


class GreetingTest(unittest.TestCase):
    def test_greeting(self):
        frame = across._get_greeting_frame(0)
        self.assertEqual(len(frame), 1 + 3 + 3 + 1)

        msg_type, = struct.unpack_from('>B', frame, 0)
        self.assertEqual(msg_type, across._GREETING)

        python_version = struct.unpack_from('BBB', frame, 1)
        self.assertEqual(python_version, sys.version_info[:3])

        across_version = struct.unpack_from('BBB', frame, 4)
        self.assertEqual('.'.join(map(str, across_version)), across.__version__)


class ProtocolErrorChannel(across.channels.Channel):
    def __init__(self, data):
        self.__buffer = memoryview(data)

    def recv_into(self, buffer):
        nbytes = min(len(self.__buffer), len(buffer))
        buffer[:nbytes], self.__buffer = self.__buffer[:nbytes], self.__buffer[nbytes:]
        return nbytes

    def send(self, buffer):
        return len(buffer)


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
        with self.assertRaisesRegex(EOFError, 'Incomplete frame size'):
            self.__simulate_error(data=b'')
        with self.assertRaisesRegex(EOFError, 'Incomplete frame size'):
            self.__simulate_error(data=b'\0\0\0')

    def test_incomplete_frame(self):
        with self.assertRaisesRegex(EOFError, 'Incomplete frame'):
            self.__simulate_error(data=struct.pack('>I', 10))
        with self.assertRaisesRegex(EOFError, 'Incomplete frame'):
            self.__simulate_error(data=struct.pack('>I', 10) + b'abc')

    def test_invalid_result_call_id(self):
        with self.assertRaisesRegex(across.ProtocolError, 'Call not found: 404'):
            msg = across._Message()
            msg.put_uint(across._RESULT)
            msg.put_uint(404)
            self.__simulate_error(msg=msg)

    def test_incomplete_superblock(self):
        with self.assertRaisesRegex(EOFError, 'Incomplete superblock'):
            self.__simulate_error(data=b'', prepend_superblock=False)
        with self.assertRaisesRegex(EOFError, 'Incomplete superblock'):
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
