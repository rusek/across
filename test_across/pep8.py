import unittest
import across
import os.path
try:
    import pep8
except ImportError:
    pep8 = None


class Pep8Test(unittest.TestCase):
    @unittest.skipIf(pep8 is None, 'pep8 is not installed')
    def test_pep8(self):
        style = pep8.StyleGuide(max_line_length=120)
        result = style.check_files([os.path.dirname(across.__file__)])
        self.assertEqual(result.total_errors, 0)
