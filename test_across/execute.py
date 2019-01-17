import unittest
from .utils import make_connection


class ExecuteTest(unittest.TestCase):
    def setUp(self):
        self.conn = make_connection()

    def tearDown(self):
        self.conn.close()

    def test_expr(self):
        self.assertEqual(self.conn.execute('2 + 3'), 5)
        self.assertEqual(self.conn.execute('1, 2, 3'), (1, 2, 3))
        self.assertEqual(self.conn.execute('1  # comment'), 1)

    def test_stmt(self):
        self.assertIsNone(self.conn.execute(''))
        self.assertIsNone(self.conn.execute('x=1'))
        self.assertIsNone(self.conn.execute('1; pass'))
        self.assertEqual(self.conn.execute('x=1; x'), 1)
        self.assertEqual(self.conn.execute('def add(a, b): return a + b\nadd(2, 3)'), 5)

    def test_args(self):
        self.assertEqual(self.conn.execute('x', x=5), 5)
        self.assertEqual(self.conn.execute('x * y', x='a', y=3), 'aaa')

    def test_special_arg_names(self):
        self.assertEqual(self.conn.execute('self', self=5), 5)
        self.assertEqual(self.conn.execute('source', source=5), 5)

    def test_no_shared_state(self):
        self.conn.execute('x = 5')
        with self.assertRaises(NameError):
            self.conn.execute('x')
