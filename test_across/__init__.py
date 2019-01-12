import unittest
from . import (
    singlethreaded,
    multithreaded,
    getconn,
    error,
    process,
    proxy,
    greeting,
    queue,
    exception,
    logging,
    pep8,
    bootstrap,
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
        queue,
        exception,
        logging,
        pep8,
        bootstrap,
    ]:
        suite.addTests(loader.loadTestsFromModule(mod))
    return suite
