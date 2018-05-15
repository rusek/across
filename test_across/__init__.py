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
    logging,
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
        logging,
    ]:
        suite.addTests(loader.loadTestsFromModule(mod))
    return suite
