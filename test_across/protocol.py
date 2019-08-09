import unittest
import struct
import zlib

import across
from across._channels import Channel


class ProtocolTest(unittest.TestCase):
    def test_magic(self):
        self.assertEqual(across._MAGIC, zlib.crc32(b'across'))
        self.assertEqual(across._BIOS_MAGIC, zlib.crc32(b'acrossbios'))


class ProtocolErrorChannel(Channel):
    def __init__(self, data):
        self.__buffer = memoryview(data)

    def recv_into(self, buffer):
        nbytes = min(len(self.__buffer), len(buffer))
        buffer[:nbytes], self.__buffer = self.__buffer[:nbytes], self.__buffer[nbytes:]
        return nbytes

    def send(self, buffer):
        return len(buffer)


def msg_to_bytes(msg):
    buffers = msg.to_buffers()
    assert len(buffers) == 1  # other cases are currently not handled
    return struct.pack('>Q', len(buffers[0]) << 1) + buffers[0]


class ProtocolErrorTest(unittest.TestCase):
    def __simulate_error(self, msg=None, data=None, prepend_superblock=True, prepend_greeting=True):
        if msg is not None:
            data = msg_to_bytes(msg)
        if prepend_superblock:
            if prepend_greeting:
                greeting_msg = across._Message()
                greeting_msg.put_uint(across._GREETING)
                greeting_msg.put_uint(0)
                data = msg_to_bytes(greeting_msg) + data
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
            self.__simulate_error(data=struct.pack('>Q', 10 << 1))
        with self.assertRaisesRegex(EOFError, 'Incomplete frame'):
            self.__simulate_error(data=struct.pack('>Q', 10 << 1) + b'abc')

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

    def test_incompatible_peer(self):
        with self.assertRaisesRegex(
            across.ProtocolError,
            'Local across .* is not compatible with remote 78.98.55'
        ):
            superblock = struct.pack('>IBBB', across._MAGIC, 78, 98, 55) + b'\0' * (across._SUPERBLOCK_SIZE - 7)
            self.__simulate_error(data=superblock, prepend_superblock=False)

    def test_incompatible_bios_peer(self):
        with self.assertRaisesRegex(
            across.ProtocolError,
            'At least across 78.98.55 is needed to bootstrap connection'
        ):
            superblock = struct.pack('>IBBB', across._BIOS_MAGIC, 78, 98, 55) + b'\0' * (across._SUPERBLOCK_SIZE - 7)
            self.__simulate_error(data=superblock, prepend_superblock=False)

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
