import unittest
import os.path

try:
    import pycodestyle
except ImportError:
    pycodestyle = None

import across


class Pep8Test(unittest.TestCase):
    @unittest.skipIf(pycodestyle is None, 'pycodestyle is not installed')
    def test_pep8(self):
        style = pycodestyle.StyleGuide(max_line_length=120)
        result = style.check_files([os.path.dirname(across.__file__)])
        self.assertEqual(result.total_errors, 0)
