import unittest
import across
import across.channels
import sys
import traceback
import types
import importlib.util
import os.path
import subprocess
from across._importer import _get_remote_loader, _compile_safe_main as compile_safe_main
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
        raise AssertionError('Importing {} should fail'.format(name))


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

    modname = '{}_{}'.format(__name__, fake_mod_counter)
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


def create_script(script):
    script = """
import sys
sys.path.insert(0, {!r})
""".format(os.path.dirname(os.path.dirname(__file__))) + script

    path = mktemp()
    os.mkdir(path)
    path = os.path.join(path, 'script.py')
    with open(path, 'w') as fh:
        fh.write(script)
    return path


class MainTest(unittest.TestCase):
    def test_main_file(self):
        path = create_script("""
from test_across.bootstrap import boot_connection
def func(): return 42
if __name__ == '__main__':
    with boot_connection() as conn:
        if conn.call(func) != 42: raise AssertionError
""")
        subprocess.check_call([sys.executable, path])

    def test_main_module(self):
        path = create_script("""
from test_across.bootstrap import boot_connection
def func(): return 42
if __name__ == '__main__':
    with boot_connection() as conn:
        if conn.call(func) != 42: raise AssertionError
""")
        subprocess.check_call(
            [sys.executable, '-m', os.path.splitext(os.path.basename(path))[0]],
            cwd=os.path.dirname(path),
        )

    def test_nested_connections(self):
        path = create_script("""
from test_across.bootstrap import boot_connection
def func(depth):
    if depth == 0: return 42
    else:
        with boot_connection() as conn: return conn.call(func, depth - 1)
if __name__ == '__main__':
    if func(2) != 42: raise AssertionError
""")
        subprocess.check_call([sys.executable, path])

    def test_traceback(self):
        path = create_script("""
from test_across.bootstrap import boot_connection
import traceback, sys
def func(): raise ValueError
if __name__ == '__main__':
    with boot_connection() as conn:
        try:
            conn.call(func)
            raise AssertionError('Exception not raised')
        except ValueError:
            formatted_tb = ''.join(traceback.format_exception(*sys.exc_info()))
            if 'def func(): raise ValueError' not in formatted_tb:
                raise AssertionError('Bad traceback: {}'.format(formatted_tb))
""")
        subprocess.check_call([sys.executable, path])

    def test_unsafe_main(self):
        path = create_script("""
from test_across.bootstrap import boot_connection
from across import OperationError
def func(): return 1
with boot_connection() as conn:
    try:
        conn.call(func)
        raise AssertionError('Exception not raised')
    except OperationError as err:
        if '__main__ module cannot be safely imported remotely' not in str(err): raise
""")
        subprocess.check_call([sys.executable, path])


class SafeMainTest(unittest.TestCase):
    def test_safe(self):
        for source in [
            'if __name__ == "__main__": pass',
        ]:
            self.assertIsNotNone(compile_safe_main(source, '<string>'))

    def test_unsafe(self):
        for source in [
            '',
            'pass',
            'id(0)',
            'if 1: pass',
            'if 2 + 2: pass',
            'if 2 < 2: pass',
            'if __name__ != "__main__": pass',
            'if __name__ == __main__: pass',
            'if __bad_name__ == "__main__": pass',
            'if __name__ == "__bad_main__": pass',
            'if __name__ == 42: pass',
            'if __name__ == "__main__" == __name__: pass',
        ]:
            self.assertIsNone(compile_safe_main(source, '<string>'), source)
