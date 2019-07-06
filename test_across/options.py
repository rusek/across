import unittest
import copy
import pickle

from across import Options


class OptionsTest(unittest.TestCase):
    def test_construct(self):
        options = Options(timeout=77)
        self.assertEqual(options.timeout, 77)

    def test_copy(self):
        options1 = Options(timeout=1)
        options2 = options1.copy()
        options3 = options1.copy(timeout=2)
        self.assertEqual(options1.timeout, 1)
        self.assertEqual(options2.timeout, 1)
        self.assertEqual(options3.timeout, 2)

    def test_deep_copy(self):
        options1 = Options(timeout=1)
        options2 = copy.deepcopy(options1)
        self.assertNotEqual(id(options1), id(options2))

    def test_pickling(self):
        options1 = Options(timeout=1)
        options2 = pickle.loads(pickle.dumps(options1))
        self.assertEqual(options1.timeout, options2.timeout)

    def test_bad_options(self):
        with self.assertRaises(AttributeError):
            Options(bad_option=None)
        with self.assertRaises(AttributeError):
            Options().copy(bad_option=None)
        with self.assertRaises(AttributeError):
            options = Options()
            options.bad_option = None
