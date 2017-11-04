import unittest
from . import singlethreaded, multithreaded, getconn


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for mod in [singlethreaded, multithreaded, getconn]:
        suite.addTests(loader.loadTestsFromModule(mod))
    return suite
