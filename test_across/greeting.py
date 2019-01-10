import unittest
import across
import struct
import sys


class GreetingTest(unittest.TestCase):
    def test_greeting(self):
        frame = across._get_greeting_frame()
        self.assertEqual(len(frame), 1 + 4 + 3 + 3)

        msg_type, = struct.unpack_from('>B', frame, 0)
        self.assertEqual(msg_type, across._GREETING)

        magic, = struct.unpack_from('>I', frame, 1)
        self.assertEqual(magic, across._greeting_magic)

        python_version = struct.unpack_from('BBB', frame, 5)
        self.assertEqual(python_version, sys.version_info[:3])

        across_version = struct.unpack_from('BBB', frame, 8)
        self.assertEqual('.'.join(map(str, across_version)), across.__version__)
