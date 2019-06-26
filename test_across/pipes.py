import across

import unittest
import os
import operator


def make_pipe():
    read_fd, write_fd = os.pipe()
    return os.fdopen(read_fd, 'rb'), os.fdopen(write_fd, 'wb')


class PipesTest(unittest.TestCase):
    def test_from_pipes(self):
        pipe1 = make_pipe()
        pipe2 = make_pipe()
        conn1 = across.Connection.from_pipes(pipe1[0], pipe2[1])
        conn2 = across.Connection.from_pipes(pipe2[0], pipe1[1])
        self.assertEqual(conn1.call(operator.add, 1, 2), 3)
        self.assertEqual(conn2.call(operator.add, 1, 2), 3)
        conn1.close()
        conn2.close()
        self.assertTrue(pipe1[0].closed)
        self.assertTrue(pipe1[1].closed)
        self.assertTrue(pipe2[0].closed)
        self.assertTrue(pipe2[1].closed)
