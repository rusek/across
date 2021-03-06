import unittest
import sys
import traceback
import types
import importlib.util
import importlib.machinery
import os.path
import subprocess
import io
import pathlib

try:
    import importlib.resources
    has_resource_reader = True
except ImportError:
    has_resource_reader = False


from across import Connection, Options, get_bios, set_debug_level
from across._channels import ProcessChannel
from across._utils import get_debug_level
from across._importer import _compile_safe_main as compile_safe_main

from .utils import (
    mktemp, make_connection, call_process, call_process_with_stderr, logging_error_marker, make_channel_pair)


def subtract(left, right):
    return left - right


top_name = __name__.partition('.')[0]


def boot_connection(export=top_name, cwd=None):
    if cwd is None:
        cwd = mktemp()
        os.mkdir(cwd)
    chan = ProcessChannel([
        sys.executable,
        '-c',
        get_bios(),
    ], cwd=cwd)
    conn = Connection(chan)
    if export is None:
        export = []
    elif isinstance(export, str):
        export = [export]
    conn.export(*export)
    return conn


def _import_nonexistent(name):
    try:
        __import__(name)
    except ImportError:
        pass
    else:
        raise AssertionError('Importing {} should fail'.format(name))


def make_files(files, base=''):
    for path, contents in files.items():
        path = os.path.join(base, path)
        if isinstance(contents, dict):
            os.mkdir(path)
            make_files(contents, base=path)
        else:  # str
            with open(path, 'w') as handle:
                handle.write(contents)


fake_mod_counter = 0


def make_fake_module_name():
    global fake_mod_counter

    modname = '{}_{}'.format(__name__.partition('.')[0], fake_mod_counter)
    fake_mod_counter += 1
    return modname


def make_fake_module(source=None, filename=None, is_package=False, name=None, resources=None):
    class Loader:
        def get_source(self, fullname):
            return source

        def is_package(self, fullname):
            return is_package

        def get_filename(self, fullname):
            return filename

        if resources is not None:
            def get_resource_reader(self, fullname):
                return self

        def open_resource(self, resource):
            data = resources.get(resource)
            if data is None:
                raise FileNotFoundError
            return io.BytesIO(data)

        def resource_path(self, resource):
            raise FileNotFoundError

        def is_resource(self, name):
            return name in resources

        def contents(self):
            return iter(resources.keys())

    if source is None:
        source = ''
    if name is None:
        name = make_fake_module_name()
    if filename is None:
        filename = '<string>'
    module = sys.modules[name] = types.ModuleType(name)
    module.__spec__ = importlib.machinery.ModuleSpec(name, Loader(), origin=filename, is_package=is_package)
    module.__file__ = filename
    module.__path__ = module.__spec__.submodule_search_locations
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

    def test_exporting_submodule_fails(self):
        assert '.' in __name__  # sanity check
        with boot_connection() as conn:
            self.assertRaises(ValueError, conn.export, __name__)

    def test_exporting_over_non_bootstrapped_connection_fails(self):
        with make_connection() as conn:
            self.assertRaises(ValueError, conn.export, top_name)

    def test_export_over_multiple_connections(self):
        # This test modifies global state (across._importer._finder.set_connection()), so it must be run
        # in a dedicated process.
        call_process(self._run_export_over_multiple_connections_test)

    @staticmethod
    def _run_export_over_multiple_connections_test():
        options = Options(accept_exported_modules=True)
        conn1 = Connection(make_channel_pair()[0], options=options)
        try:
            Connection(make_channel_pair()[0], options=options)
        except ValueError:
            pass
        else:
            raise AssertionError('ValueError not raised')
        conn1.cancel()
        try:
            conn1.close()
        except Exception:  # generated by cancel()
            pass

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

    def test_exporting_already_exported_module_has_no_effect(self):
        with boot_connection(None) as conn:
            conn.export(top_name)
            conn.export(top_name)
            self.assertEqual(conn.call(subtract, 5, 3), 2)
            conn.export(top_name)

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
        with boot_connection(module.__name__) as conn:
            try:
                conn.call(module.func)
            except module.FuncError:
                formatted_tb = ''.join(traceback.format_exception(*sys.exc_info()))
                self.assertIn(source_lines[1], formatted_tb)
            else:
                self.fail('Exception not raised')

    def test_dummy_filenames_are_left_unchanged(self):
        dummy_filename = '<string>'
        module = make_fake_module('def func(): return __file__', dummy_filename)
        with boot_connection(module.__name__) as conn:
            self.assertEqual(conn.call(module.func), dummy_filename)

    def test_filenames_are_not_repeatedly_mangled(self):
        path = mktemp()
        source = """
from test_across.importer import boot_connection
def get_filename(): return __file__
def func():
    with boot_connection([__name__, 'test_across']) as conn: return get_filename(), conn.call(get_filename)
        """
        module = make_fake_module(source, path)
        with boot_connection([top_name, module.__name__]) as conn:
            first_filename, second_filename = conn.call(module.func)
            self.assertEqual(first_filename, second_filename)

    def test_local_modules_take_precendence(self):
        path = mktemp()
        make_files({path: {top_name + '.py': ''}})
        with boot_connection(cwd=path) as conn:
            self.assertEqual(conn.call(subtract, 4, 3), 1)

    def test_remote_submodules_are_not_available_when_using_local_modules(self):
        path = mktemp()
        module_name = make_fake_module_name()
        remote_submodule_name = 'submod'

        make_files({path: {module_name: {'__init__.py': 'remote=True', '{}.py'.format(remote_submodule_name): ''}}})
        make_fake_module(
            'remote=False',
            filename=os.path.join(path, module_name, '__init__.py'),
            is_package=True,
            name=module_name
        )
        with boot_connection(module_name) as conn:
            self.assertFalse(conn.execute('from {} import remote; remote'.format(module_name)))  # sanity check
            with self.assertRaises(ImportError):
                conn.execute('import {}.{}'.format(module_name, remote_submodule_name))

    def test_exporting_is_not_allowed_when_module_is_remotely_imported(self):
        path = mktemp()
        make_files({path: {top_name + '.py': ''}})
        with boot_connection(None, cwd=path) as conn:
            conn.execute('import {}'.format(top_name))
            with self.assertRaisesRegex(ValueError, 'Cannot export module .* because it is already imported'):
                conn.export(top_name)

    def test_local_submodules_are_not_used_when_using_remote_module(self):
        module = make_fake_module('', is_package=True)
        submodule = make_fake_module('', name='{}.submod'.format(module.__name__))
        with boot_connection([top_name, module.__name__]) as conn:
            conn.call_ref(make_fake_module, '', is_package=True, name=module.__name__)
            with self.assertRaises(ImportError):
                conn.execute('import {}'.format(submodule.__name__))

    def test_implicit_namespace_packages(self):
        path = mktemp()
        module_name = make_fake_module_name()
        make_files({path: {
            'dir1': {module_name: {'submod1.py': 'x=1'}},
            'dir2': {module_name: {'submod2.py': 'x=2'}},
        }})
        orig_sys_path = sys.path[:]
        sys.path[:0] = [os.path.join(path, 'dir1'), os.path.join(path, 'dir2')]
        try:
            with boot_connection(module_name) as conn:
                self.assertEqual(conn.execute('from {}.submod1 import x; x'.format(module_name)), 1)
                self.assertEqual(conn.execute('from {}.submod2 import x; x'.format(module_name)), 2)
                self.assertRaises(ImportError, conn.execute, 'from {} import submod3'.format(module_name))
        finally:
            sys.path[:] = orig_sys_path


def get_script_preambule():
    return 'import sys; sys.path.insert(0, {!r})\n'.format(os.path.dirname(os.path.dirname(__file__)))


def create_script(script):
    path = mktemp()
    os.mkdir(path)
    path = os.path.join(path, 'script.py')
    with open(path, 'w') as fh:
        fh.write(get_script_preambule() + script)
    return path


class MainTest(unittest.TestCase):
    def test_main_file(self):
        path = create_script("""
from test_across.importer import boot_connection
def func(): return 42
if __name__ == '__main__':
    with boot_connection(__name__) as conn:
        if conn.call(func) != 42: raise AssertionError
""")
        subprocess.check_call([sys.executable, path])

    def test_main_module(self):
        path = create_script("""
from test_across.importer import boot_connection
def func(): return 42
if __name__ == '__main__':
    with boot_connection(__name__) as conn:
        if conn.call(func) != 42: raise AssertionError
""")
        subprocess.check_call(
            [sys.executable, '-m', os.path.splitext(os.path.basename(path))[0]],
            cwd=os.path.dirname(path),
        )

    def test_nested_connections(self):
        path = create_script("""
from test_across.importer import boot_connection
def func(depth):
    if depth == 0: return 42
    else:
        with boot_connection(__name__) as conn: return conn.call(func, depth - 1)
if __name__ == '__main__':
    if func(2) != 42: raise AssertionError
""")
        subprocess.check_call([sys.executable, path])

    def test_traceback(self):
        path = create_script("""
from test_across.importer import boot_connection
import traceback, sys
def func(): raise ValueError
if __name__ == '__main__':
    with boot_connection(__name__) as conn:
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
from test_across.importer import boot_connection
from across import OperationError
def func(): return 1
with boot_connection(__name__) as conn:
    try:
        conn.call(func)
        raise AssertionError('Exception not raised')
    except OperationError as err:
        err = err.__context__
        if not isinstance(err, ImportError): raise
        if '__main__ module cannot be safely imported remotely' not in str(err): raise
""")
        subprocess.check_call([sys.executable, path])

    def test_exporting_main_when_already_exported_has_no_effect(self):
        path = create_script("""
from test_across.importer import boot_connection
import itertools
counter = itertools.count()
def func(expected):
    value = next(counter)
    if value != expected: raise AssertionError((value, expected))
if __name__ == '__main__':
    with boot_connection(__name__) as conn:
        conn.call(func, 0)
        conn.call(func, 1)
        conn.export(__name__)
        conn.call(func, 2)
""")
        subprocess.check_call([sys.executable, path])

    def test_main_py_relative_imports(self):
        path = mktemp()
        module_name = make_fake_module_name()
        make_files({path: {module_name: {
            '__init__.py': '',
            '__main__.py': get_script_preambule() + """
from test_across.importer import boot_connection
from .submod import func_in_submod
def func_in_main(): func_in_submod()
if __name__ == '__main__':
    with boot_connection(['__main__', __package__]) as conn:
        conn.call(func_in_main)
            """,
            'submod.py': 'def func_in_submod(): pass',
        }}})
        subprocess.check_call([sys.executable, '-m', module_name], cwd=path)


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


class DebugTest(unittest.TestCase):
    def test_debug_level(self):
        _, stderr = call_process_with_stderr(_set_debug_level_and_boot_connection)
        self.assertNotIn(logging_error_marker, stderr)


def _set_debug_level_and_boot_connection():
    set_debug_level(10)
    with boot_connection() as conn:
        if conn.call(get_debug_level) != 10:
            raise AssertionError


def _read_by_path(module_name, resource):
    with importlib.resources.path(module_name, resource) as path:
        with open(path, 'rb') as fh:
            return fh.read()


@unittest.skipUnless(has_resource_reader, 'Resource reader API is not available')
class ResourceReaderTest(unittest.TestCase):
    DATA1_FILE = 'data1.txt'

    def test_reading_resources_on_native_loader(self):
        resource = self.DATA1_FILE
        data = importlib.resources.read_binary(__package__, resource)
        with boot_connection() as conn:
            self.assertEqual(conn.call(importlib.resources.read_binary, __package__, resource), data)
            self.assertRaises(FileNotFoundError, conn.call, importlib.resources.read_binary, __package__,
                resource + '.or.not')
            self.assertEqual(conn.call(importlib.resources.read_text, __package__, resource), data.decode('utf-8'))
            self.assertEqual(conn.call(importlib.resources.is_resource, __package__, resource), True)
            self.assertEqual(conn.call(importlib.resources.is_resource, __package__, resource + '.or.not'), False)
            self.assertIn(resource, list(conn.call_ref(importlib.resources.contents, __package__)))
            self.assertEqual(conn.call(_read_by_path, __package__, resource), data)

    def test_reading_resources_on_custom_loader(self):
        data = b'somedata'
        resource = 'someresource'
        module = make_fake_module(is_package=True, resources={resource: data})
        with boot_connection(module.__name__) as conn:
            self.assertEqual(conn.call(importlib.resources.read_binary, module.__name__, resource), data)
            self.assertRaises(FileNotFoundError, conn.call, importlib.resources.read_binary, module.__name__,
                resource + '.or.not')
            self.assertEqual(conn.call(importlib.resources.read_text, module.__name__, resource), data.decode('utf-8'))
            self.assertEqual(conn.call(importlib.resources.is_resource, module.__name__, resource), True)
            self.assertEqual(conn.call(importlib.resources.is_resource, module.__name__, resource + '.or.not'), False)
            self.assertEqual(list(conn.call_ref(importlib.resources.contents, module.__name__)), [resource])

    def test_get_resource_reader_on_non_package(self):
        module = make_fake_module()
        with boot_connection(module.__name__) as conn:
            self.assertTrue(conn.execute("""
import importlib.util as u
u.find_spec(m).loader.get_resource_reader(m) is None
            """, m=module.__name__))

    def test_path_does_not_use_local_file(self):
        resource = self.DATA1_FILE
        data = b'expected data'
        module = make_fake_module(
            is_package=True,
            filename=sys.modules[__package__].__file__,
            resources={resource: data}
        )
        with boot_connection(module.__name__) as conn:
            actual_data = conn.execute("""
import importlib.resources as r
with r.path(module_name, resource) as path:
    with open(path, 'rb') as fh:
        data = fh.read()
data
            """, module_name=module.__name__, resource=resource)
            self.assertEqual(actual_data, data)

    # Tests for across._importer._path replacement function.
    def test_path_implementation(self):
        self._test_path_implementation()
        with boot_connection() as conn:
            conn.call(self._test_path_implementation)

    @staticmethod
    def _test_path_implementation():
        def check_exc(package, resource, exc_type):
            try:
                with importlib.resources.path(package, resource):
                    pass
            except exc_type:
                pass
            else:
                raise AssertionError('Exception not raised')

        check_exc('importlib.resources', 'resources.py', TypeError)
        check_exc('importlib', 'some/file.py', ValueError)

        def check_ok(package, resource):
            with importlib.resources.path(package, resource) as path:
                if not isinstance(path, pathlib.Path):
                    raise AssertionError('Not a Path object: {!r}'.format(path))

        check_ok('importlib', 'resources.py')
        check_ok(importlib.import_module('importlib'), 'resources.py')

        module = make_fake_module(filename=importlib.__file__, is_package=True)
        module.__spec__.has_location = False
        check_exc(module.__name__, 'resources.py', FileNotFoundError)
        module.__spec__.has_location = True
        check_ok(module.__name__, 'resources.py')

        module = make_fake_module(is_package=True, resources={'file': b'data'})
        with importlib.resources.path(module.__name__, 'file') as path:
            os.unlink(path)
