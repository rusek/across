import unittest
from . import singlethreaded, multithreaded, getconn, error


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for mod in [singlethreaded, multithreaded, getconn, error]:
        suite.addTests(loader.loadTestsFromModule(mod))
    return suite
