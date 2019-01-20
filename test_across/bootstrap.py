import unittest
import across
import across.channels
import sys
import traceback
import types
import importlib.util
from across._importer import _get_remote_loader
from .utils import mktemp


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


fake_mod_counter = 0

def make_fake_module(source, filename):
    class Loader:
        def get_source(self, fullname):
            return source

        def is_package(self, fullname):
            return False

        def get_filename(self, fullname):
            return filename

    global fake_mod_counter

    modname = '%s_%s' % (__name__, fake_mod_counter)
    fake_mod_counter += 1

    module = sys.modules[modname] = types.ModuleType(modname)
    module.__spec__ = importlib.util.spec_from_loader(__name__, Loader())
    exec(compile(source, filename, 'exec'), module.__dict__)
    return module


class SimulatedError(Exception):
    pass


def raise_simulated_error():
    raise SimulatedError


class BootstrapTest(unittest.TestCase):
    def test_basic(self):
        with boot_connection() as conn:
            self.assertEqual(conn.call(subtract, 5, 2), 3)

    def test_importing_non_existent_module_remotely(self):
        with boot_connection() as conn:
            conn.call(_import_nonexistent, 'across_nonexistent')
            conn.call(_import_nonexistent, 'across.nonexistent.nonexistent')

    def test_remote_traceback(self):
        with boot_connection() as conn:
            try:
                conn.call(raise_simulated_error)
            except SimulatedError:
                formatted_tb = ''.join(traceback.format_exception(*sys.exc_info()))
                self.assertIn('raise SimulatedError', formatted_tb)
            else:
                self.fail('Exception not raised')

    # This test verifies that if a local module is implemented in "/some/modfile.py", then remote process will
    # not try to use its own version of "/some/modfile.py" for obtaining source lines for traceback
    def test_remote_files_are_not_used_for_generating_remote_tracebacks(self):
        path = mktemp()
        open(path, 'w').close()

        source_lines = [
            'class FuncError(Exception): pass\n',
            'def func(): raise FuncError\n'
        ]

        module = make_fake_module(''.join(source_lines), path)
        with boot_connection() as conn:
            try:
                conn.call(module.func)
            except module.FuncError:
                formatted_tb = ''.join(traceback.format_exception(*sys.exc_info()))
                self.assertIn(source_lines[1], formatted_tb)
            else:
                self.fail('Exception not raised')


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
