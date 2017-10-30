import unittest
from . import singlethreaded


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for mod in [singlethreaded]:
        suite.addTests(loader.loadTestsFromModule(mod))
    return suite
