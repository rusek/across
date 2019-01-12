import unittest
import across
import across.channels
import sys
from across._importer import _get_remote_loader


def subtract(left, right):
    return left - right


def boot_connection():
    chan = across.channels.ProcessChannel([
        '/bin/sh',
        '-c',
        'cd / && "$0" -c "$1"',
        sys.executable,
        across.get_bios(),
    ])
    return across.Connection(chan)


def _import_nonexistent(name):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError('Importing %s should fail' % (name, ))


class BootstrapTest(unittest.TestCase):
    def test_basic(self):
        with boot_connection() as conn:
            self.assertEqual(conn.call(subtract, 5, 2), 3)

    def test_importing_non_existent_module_remotely(self):
        with boot_connection() as conn:
            conn.call(_import_nonexistent, 'across_nonexistent')
            conn.call(_import_nonexistent, 'across.nonexistent.nonexistent')


class ImporterTest(unittest.TestCase):
    def test_stdlib_is_skipped(self):
        import os
        import sys
        import traceback
        import logging.handlers

        self.assertIsNone(_get_remote_loader(os.__name__))
        self.assertIsNone(_get_remote_loader(sys.__name__))
        self.assertIsNone(_get_remote_loader(traceback.__name__))
        self.assertIsNone(_get_remote_loader(logging.handlers.__name__))
