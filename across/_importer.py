"""
Across import machinery, full of deadly traps and zombie unicorns. Stay away.
"""

import sys
import importlib
import importlib.util
import types
import ast
import pickle
import functools
import logging
import socket
import time
import io
import os
import contextlib
import pathlib
import tempfile

try:
    import importlib.resources
except ImportError:
    pass


# Basic logging utilities. Since importer needs to be self-contained, they must be implemented here.

_debug_level = 0
_debug_handler = None

logger = logging.getLogger('across')
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


def get_debug_level():
    return _debug_level


def set_debug_level(level):
    global _debug_level
    global _debug_handler

    if level <= 0:
        if _debug_handler is not None:
            logger.removeHandler(_debug_handler)
            logger.setLevel(logging.NOTSET)
            _debug_handler = None
    else:
        if _debug_handler is None:
            formatter = logging.Formatter(
                '%(asctime)s %(levelname)-5s ' + socket.gethostname().replace('%', '%%') +
                '/%(process)d %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
            )
            formatter.converter = time.gmtime

            _debug_handler = logging.StreamHandler()
            _debug_handler.setFormatter(formatter)
            logger.addHandler(_debug_handler)

        if level == 1:
            logging_level = logging.INFO
        else:
            logging_level = logging.DEBUG

        logger.setLevel(logging_level)
        _debug_handler.setLevel(logging_level)

    _debug_level = level


# Compile given source, but skip the code inside "if __name__ == '__main__'". When such "if" statement cannot
# be located, return None to indicate that the source cannot be executed without triggerring the usual effects
# on main module
def _compile_safe_main(source, filename):
    mod = compile(source, filename, 'exec', ast.PyCF_ONLY_AST, dont_inherit=True)
    if mod.body and isinstance(mod.body[-1], ast.If):
        test = mod.body[-1].test
        if (
            isinstance(test, ast.Compare) and
            isinstance(test.left, ast.Name) and
            test.left.id == '__name__' and
            len(test.ops) == 1 and
            isinstance(test.ops[0], ast.Eq) and
            isinstance(test.comparators[0], ast.Str) and
            test.comparators[0].s == '__main__'
        ):
            mod.body[-1].test = ast.Num(0, lineno=0, col_offset=0)
            return compile(mod, filename, 'exec', dont_inherit=True)

    return None


def _find_across_loader(fullname):
    if fullname == '__main__':
        # Let's deal with various quirks of runpy module:
        #   - __spec__ is None when using 'python -c script.py', we have to fall back to __loader__
        #   - __spec__.name differs from fullname when using 'python -m modname'; __spec__.name is the one
        #     expected by __spec__.__loader__
        main_mod = sys.modules.get('__main__')
        if main_mod is not None:
            parent_override = main_mod.__package__
            spec = getattr(main_mod, '__spec__', None)
            if spec is None:
                loader = getattr(main_mod, '__loader__', None)
                if loader is None:
                    raise ValueError('__main__.__loader__ is not set')
                logger.debug('Querying module %r, loader=%r', fullname, loader)
                return _interrogate_loader(loader, fullname, parent_override=parent_override)
            else:
                if spec.loader is None:
                    raise ValueError('{!r}.loader is not set'.format(spec))
                logger.debug('Querying module %r (main), spec=%r', spec.name, spec)
                return _interrogate_loader(spec.loader, spec.name, parent_override=parent_override)

    try:
        spec = importlib.util.find_spec(fullname)
    except ImportError:
        logger.debug('Module %r not found', fullname)
        return None
    if spec is None:
        logger.debug('Module %r not found', fullname)
        return None
    if spec.loader is None:
        # Yet another hack, this time for implicit namespace packages.
        if spec.submodule_search_locations:
            logger.debug('Querying module %r (namespace), spec=%r', fullname, spec)
            return AcrossLoader('', True, '<source>')
        raise ValueError('{!r}.loader is not set'.format(spec))
    logger.debug('Querying module %r, spec=%r', fullname, spec)
    return _interrogate_loader(spec.loader, fullname)


def _interrogate_loader(loader, fullname, parent_override=None):
    if isinstance(loader, AcrossLoader):
        return loader
    get_source = getattr(loader, 'get_source', None)
    if get_source is None:
        raise TypeError('Loader {!r} for module {} does not implement get_source method'.format(loader, fullname))
    source = get_source(fullname)
    if source is None:
        raise ValueError('Source is not available for loader {!r} and module {}'.format(loader, fullname))
    is_package = getattr(loader, 'is_package', None)
    if is_package is None:
        raise TypeError('Loader {!r} for module {} does not implement is_package method'.format(loader, fullname))
    package = is_package(fullname)
    get_filename = getattr(loader, 'get_filename', None)
    if get_filename is None:
        raise TypeError('Loader {!r} for module {} does not implement get_filename method'.format(loader, fullname))
    filename = get_filename(fullname)
    if filename is None:
        raise ValueError('Filename is not available for loader {!r} and module {}'.format(loader, fullname))
    return AcrossLoader(source, package, _mangle_filename(filename), parent_override=parent_override)


# Make filename of a local module safe for remote use.
#
# Dummy filenames ('<...>') are left unchanged, because such names are typically not interpreted in any way.
# For anything else, we need to ensure that the resulting filename will not point to an existing file on a remote
# machine, otherwise linecache module will use that file to obtain module source instead of using
# AcrossLoader.get_source() method. We do that by appending '*' to filename, hoping that nobody will ever create
# a file with '.py*' extension.
def _mangle_filename(filename):
    if filename.startswith('<') and filename.endswith('>'):
        return filename
    return filename + '*'


# For packages, __path__ attribute holds a list of directories where submodules are located. This object
# acts as a replacement of that list. First, it allows detecting if parent module was loaded by us. Second,
# it prevents mutating __path__ by user code.
class AcrossSearchPath:
    def __getitem__(self, item):
        raise IndexError

    def __iter__(self):
        return iter(())

    def __len__(self):
        return 0

    def __contains__(self, item):
        return False


class AcrossFinder:
    def __init__(self):
        self.__loaders = {}
        self.__conn = None
        self.__exported_modules = set()

    def export(self, modules, loaders=None):
        for name in modules:
            if '.' in name:
                raise ValueError('Not a top-level module: {}'.format(name))
            if name in sys.modules and name not in self.__exported_modules and name != '__main__':
                raise ValueError('Cannot export module {} because it is already imported'.format(name))

        had_exported_modules = bool(self.__exported_modules)
        had_main = ('__main__' in self.__exported_modules)
        self.__exported_modules.update(modules)
        if loaders is not None:
            self.__loaders.update(loaders)

        if self.__exported_modules and not had_exported_modules:
            sys.meta_path.insert(0, self)
        if '__main__' in self.__exported_modules and not had_main:
            # I originally injected fake module here with __getattr__ loading the module lazily, but that
            # didn't work correctly on CPython 3.7 (ImportError raised when _compile_safe_main returns None
            # was swallowed by interpreter).
            sys.modules.pop('__main__', None)

    def set_connection(self, conn):
        if self.__conn is not None:
            raise ValueError('Cannot import modules over multiple connections')
        self.__conn = conn

    def get_connection(self):
        if self.__conn is None:
            raise RuntimeError('Connection not initialized')
        return self.__conn

    def find_spec(self, fullname, path=None, target=None):
        loader = self.find_module(fullname, path)
        if loader is None:
            return None
        spec = importlib.util.spec_from_loader(fullname, loader)
        if spec.submodule_search_locations is not None:  # is it a package?
            spec.submodule_search_locations = AcrossSearchPath()
        return spec

    def find_module(self, fullname, path=None):
        top_name, dot, _ = fullname.partition('.')
        # Skip if topmost module is not exported
        if top_name not in self.__exported_modules:
            return None
        # Skip if parent module was not loaded by us
        if dot and not isinstance(path, AcrossSearchPath):
            return None

        if fullname not in self.__loaders:
            if self.__conn is None:
                return None
            self.__loaders[fullname] = self.__conn.call(_find_across_loader, fullname)
        return self.__loaders[fullname]


class AcrossLoader:
    # When using 'python -m somemod', parent module of '__main__' is set to 'somemod'. This is an unusual situation
    # that requires overriding the default logic for determining parent module. In such cases, 'parent_override'
    # is the name of parent module to set.
    def __init__(self, source, package, filename, parent_override=None, code=None):
        self.__source = source
        self.__package = package
        self.__filename = filename
        self.__parent_override = parent_override
        self.__code = code

    def get_filename(self, fullname):
        return self.__filename

    def get_code(self, fullname):
        if self.__code is None:
            if fullname == '__main__':
                code = _compile_safe_main(self.get_source(fullname), self.get_filename(fullname))
                if code is None:
                    raise ImportError('__main__ module cannot be safely imported remotely because it lacks '
                                      'proper "if __name__ == \'__main__\'" guard')
            else:
                code = compile(self.get_source(fullname), self.get_filename(fullname), 'exec', dont_inherit=True)
            self.__code = code
        return self.__code

    def get_source(self, fullname):
        return self.__source

    def is_package(self, fullname):
        return self.__package

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        if self.__parent_override:
            # Workaround for https://bugs.python.org/issue30876
            if sys.version_info <= (3, 5):
                __import__(self.__parent_override)
            module.__package__ = self.__parent_override
        exec(self.get_code(module.__name__), module.__dict__)

    def get_resource_reader(self, fullname):
        if not self.__package:
            return None
        return AcrossResourceReader(_finder.get_connection(), fullname)

    def __reduce__(self):
        return AcrossLoader, self.deconstruct(with_code=False)

    # For internal use only
    def deconstruct(self, with_code):
        return (self.__source, self.__package, self.__filename, self.__parent_override,
                (self.__code if with_code else None))


def _contents(fullname):
    return list(importlib.resources.contents(fullname))


class AcrossResourceReader:
    def __init__(self, conn, fullname):
        self.__conn = conn
        self.__fullname = fullname

    def open_resource(self, resource):
        return io.BytesIO(self.__conn.call(importlib.resources.read_binary, self.__fullname, resource))

    def resource_path(self, resource):
        raise FileNotFoundError

    def is_resource(self, name):
        return self.__conn.call(importlib.resources.is_resource, self.__fullname, name)

    def contents(self):
        return self.__conn.call(_contents, self.__fullname)


# This list contains 'across' module with all its dependencies, excluding importer module
# (which is a different story, because it needs to load itself).
_core_module_names = (
    'across',
    'across._utils',
    'across._channels',
)


def get_bootloader(func, *args):
    own_loader = _find_across_loader(__name__)
    if own_loader is None:
        raise ImportError('{} not found'.format(__name__))

    loaders = {}
    for module_name in _core_module_names:
        loader = _find_across_loader(module_name)
        if loader is None:
            raise ImportError('{} not found'.format(module_name))
        loaders[module_name] = loader.deconstruct(with_code=False)

    data = {
        'own_loader': own_loader.deconstruct(with_code=False),
        'ns': {'__name__': __name__},
        'loaders': loaders,
        'debug_level': get_debug_level(),
        'func_with_args': pickle.dumps((func, args), protocol=3),
    }

    # If we compile the code without providing filename, coverage for _bootstrap() function will not be computed.
    return (
        "__across_boot={!r};exec(compile(__across_boot['own_loader'][0],__across_boot['own_loader'][2],'exec',"
        "dont_inherit=True),__across_boot['ns']);__across_boot=__across_boot['ns']['_bootstrap'](__across_boot);"
        "__across_boot(ACROSS)\n".format(data)
    )


def _module_from_spec(spec):
    module = types.ModuleType(spec.name)
    module.__spec__ = spec
    module.__file__ = spec.origin
    module.__loader__ = spec.loader
    module.__package__ = spec.parent
    if spec.submodule_search_locations is not None:
        module.__path__ = spec.submodule_search_locations
    return module


_finder = AcrossFinder()


def get_finder():
    return _finder


def _bootstrap(data):
    # Enable debug mode inside bootloader as soon as possible.
    debug_level = data['debug_level']
    set_debug_level(debug_level)

    logger.debug('Bootloader starts, python=%r', sys.version)

    # The environment in which we are currently running code is not a proper Python module: __file__/__package__
    # are not set, there is no entry in sys.modules, etc. First step is to create our own module.
    if __name__ in sys.modules:
        raise RuntimeError('{} already in sys.modules'.format(__name__))
    # Beware: AcrossLoader.__module__ is invalid. We will have to recreate loader object later.
    tmp_loader = AcrossLoader(*data['own_loader'])
    own_spec = importlib.util.spec_from_loader(__name__, tmp_loader)
    own_module = sys.modules[__name__] = _module_from_spec(own_spec)
    tmp_loader.exec_module(own_module)

    # Our own module is ready now. We should stop using current globals and switch to the ones from 'own_module'.
    # Start with switching debug mode.
    set_debug_level(0)
    own_module.set_debug_level(debug_level)

    # Recreate our own loader.
    own_loader = own_module.AcrossLoader(*tmp_loader.deconstruct(with_code=True))
    own_module.__loader__ = own_spec.loader = own_loader

    # The most tricky part is behind us. Now let's set up finder object for loading remaining modules.
    loaders = dict(
        (module_name, own_module.AcrossLoader(*args))
        for module_name, args in data['loaders'].items()
    )
    loaders[__name__] = own_loader
    own_module.get_finder().export(['across'], loaders=loaders)

    # Now we can finally load startup function that takes care of creating Connection object.
    func, args = pickle.loads(data['func_with_args'])
    func_with_args = functools.partial(func, *args)

    # Apply some additional workarounds.
    _patch_resources_path()

    logger.debug('Bootloader ends')

    return func_with_args


def _patch_resources_path():
    if (3, 7) <= sys.version_info < (3, 9):
        functools.update_wrapper(_path, importlib.resources.path)
        importlib.resources.path = _path


# Fixed implementation of importlib.resources.path. See https://bugs.python.org/issue39980
@contextlib.contextmanager
def _path(package, resource):
    if os.path.split(resource)[0]:
        raise ValueError('{!r} must be only a file name'.format(resource))
    if isinstance(package, str):
        package = importlib.import_module(package)
    spec = package.__spec__
    if spec.submodule_search_locations is None:
        raise TypeError('{!r} is not a package'.format(spec.name))
    get_resource_reader = getattr(spec.loader, 'get_resource_reader', None)
    if get_resource_reader is not None:
        resource_reader = get_resource_reader(spec.name)
    else:
        resource_reader = None
    if resource_reader is not None:
        try:
            path = resource_reader.resource_path(resource)
        except FileNotFoundError:
            pass
        else:
            yield pathlib.Path(path)
            return
    else:
        if spec.origin is None or not spec.has_location:
            raise FileNotFoundError('Package has no location {!r}'.format(package))
        path = pathlib.Path(spec.origin).parent / resource

        if path.exists():
            yield path
            return
    fd, path = tempfile.mkstemp()
    try:
        with open(fd, 'wb') as fh:
            with importlib.resources.open_binary(package, resource) as fh2:
                fh.write(fh2.read())
        yield pathlib.Path(path)
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
