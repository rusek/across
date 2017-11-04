import unittest
import across
import operator


class ProcessTestCase(unittest.TestCase):
    def test_without_args(self):
        with across.Connection(across.ProcessChannel()) as conn:
            self.assertEqual(conn.call(operator.add, 2, 2), 4)
