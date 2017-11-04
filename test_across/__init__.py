import unittest
from . import singlethreaded, getconn


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for mod in [singlethreaded, getconn]:
        suite.addTests(loader.loadTestsFromModule(mod))
    return suite
