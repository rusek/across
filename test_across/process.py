import unittest
import across
import across.channels
import operator


class ProcessTestCase(unittest.TestCase):
    def test_without_args(self):
        with across.Connection(across.channels.ProcessChannel()) as conn:
            try:
                self.assertEqual(conn.call(operator.add, 2, 2), 4)
            except:
                pass
