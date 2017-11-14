import unittest
import across
import struct
import sys


class GreetingTest(unittest.TestCase):
    def test_greeting(self):
        frame = across._get_greeting_frame()
        self.assertEqual(len(frame), 4 + 4 + 1 + 3 + 3)

        size, msg_type = struct.unpack_from('>IB', frame, 0)
        self.assertEqual(size, len(frame) - 4)
        self.assertEqual(msg_type, across._GREETING)

        magic, = struct.unpack_from('>I', frame, 5)
        self.assertEqual(magic, across._greeting_magic)

        python_version = struct.unpack_from('BBB', frame, 9)
        self.assertEqual(python_version, sys.version_info[:3])

        across_version = struct.unpack_from('BBB', frame, 12)
        self.assertEqual('.'.join(map(str, across_version)), across.__version__)
