import unittest
from . import (
    singlethreaded,
    multithreaded,
    getconn,
    error,
    process,
    proxy,
    greeting,
    elsewhere,
    exception,
)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for mod in [
        singlethreaded,
        multithreaded,
        getconn,
        error,
        process,
        proxy,
        greeting,
        elsewhere,
        exception,
    ]:
        suite.addTests(loader.loadTestsFromModule(mod))
    return suite
