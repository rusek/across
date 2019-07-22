import unittest
import os.path

try:
    import pycodestyle
except ImportError:
    pycodestyle = None

import across


options = dict(
    max_line_length=120,
    ignore=[
        'W504',  # line break after binary operator
        'E722',  # do not use bare 'except'
    ],
)


class Pep8Test(unittest.TestCase):
    @unittest.skipIf(pycodestyle is None, 'pycodestyle is not installed')
    def test_pep8(self):
        style = pycodestyle.StyleGuide(**options)
        result = style.check_files([os.path.dirname(across.__file__)])
        self.assertEqual(result.total_errors, 0)
