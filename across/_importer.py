import sys
import importlib
import importlib.util
import types
import sysconfig
import os.path
import ast
import threading


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


def _get_loader_with_fullname(fullname):
    if fullname == '__main__':
        # Let's deal with various quirks of runpy module:
        #   - __spec__ is None when using 'python -c script.py', we have to fall back to __loader__
        #   - __spec__.name differs from fullname when using 'python -m modname'; __spec__.name is the one
        #     expected by __spec__.__loader__
        main_mod = sys.modules.get('__main__')
        if main_mod is not None:
            spec = getattr(main_mod, '__spec__', None)
            if spec is None:
                loader = getattr(main_mod, '__loader__', None)
                if loader is None:
                    raise ValueError('__main__.__loader__ is not set')
                return loader, fullname
            else:
                if spec.loader is None:
                    raise ValueError('{!r}.loader is not set'.format(spec))
                return spec.loader, spec.name

    try:
        spec = importlib.util.find_spec(fullname)
    except ImportError:
        return None, None
    if spec is None:
        return None, None
    if spec.loader is None:
        raise ValueError('{!r}.loader is not set'.format(spec))
    return spec.loader, fullname


def _is_stdlib_module(fullname):
    fullname = fullname.partition('.')[0]

    if fullname in sys.builtin_module_names:
        return True

    __import__(fullname)
    path = sys.modules[fullname].__file__

    if not path or not os.path.exists(path):
        return False

    stdlib_dirs = [sysconfig.get_path('stdlib'), sysconfig.get_path('platstdlib')]

    parent_path = os.path.dirname(path)
    if parent_path in stdlib_dirs:
        return True

    if os.path.exists(os.path.join(parent_path, '__init__.py')) and os.path.dirname(parent_path) in stdlib_dirs:
        return True

    return False


def _get_remote_loader(fullname):
    loader, fullname = _get_loader_with_fullname(fullname)
    # this (checking if module exists) must go before _is_stdlib_module
    if loader is None:
        return None
    if _is_stdlib_module(fullname):
        return None
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
    return _RemoteLoader(source, package, filename)


class _RemoteFinder(object):
    def __init__(self, loaders):
        self.__loaders = loaders
        self.__conn = None

    def set_connection(self, conn):
        self.__conn = conn

    def find_spec(self, fullname, path=None, target=None):
        loader = self.find_module(fullname)
        if loader is None:
            return None
        return importlib.util.spec_from_loader(fullname, loader)

    def find_module(self, fullname, path=None):
        if fullname not in self.__loaders:
            if '.' in fullname and fullname.rpartition('.')[0] not in self.__loaders:
                loader = None
            elif self.__conn is None:
                loader = None
            else:
                loader = self.__conn.call(_get_remote_loader, fullname)
            self.__loaders[fullname] = loader
        return self.__loaders[fullname]


class _RemoteLoader(object):
    def __init__(self, source, package, filename, code=None):
        self.__source = source
        self.__package = package
        self.__filename = filename
        self.__code = code

    def get_filename(self, fullname):
        # We need to apply some sort of mangling to prevent clash with local files. Appending '*' seems like
        # a good solution, because it's rather unlikely that anyone else would create a file with '.py*' extension.
        return self.__filename + '*'

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
        exec(self.get_code(module.__name__), module.__dict__)

    def deconstruct(self):
        return self.__source, self.__package, self.__filename, self.__code


_minimal_modules = (
    'across',
    'across.channels',
    'across._importer',
)


def get_bootstrap_line():
    modules = {}
    for fullname in _minimal_modules:
        loader = _get_remote_loader(fullname)
        if loader is None:
            raise ImportError('{} not found'.format(fullname))
        modules[fullname] = loader.deconstruct()
    return ("__across_vars={!r},{!r},{{}};exec(__across_vars[0][__across_vars[1]][0],__across_vars[2]);"
            "__across_vars[2]['_bootstrap'](__across_vars[0],__across_vars[1])\n".format(modules, __name__))


def _module_from_spec(spec):
    module = types.ModuleType(spec.name)
    module.__spec__ = spec
    module.__file__ = spec.origin
    module.__loader__ = spec.loader
    module.__package__ = spec.parent
    if spec.submodule_search_locations is not None:
        module.__path__ = spec.submodule_search_locations
    return module


class _LazyMain:
    def __init__(self, finder):
        self.__finder = finder
        self.__lock = threading.Lock()

    def __getattr__(self, item):
        with self.__lock:
            module = sys.modules['__main__']
            if module is self:
                spec = self.__finder.find_spec('__main__')
                if spec is None:
                    raise ImportError('No module named __main__')
                module = sys.modules['__main__'] = _module_from_spec(spec)
                spec.loader.exec_module(module)
        return getattr(module, item)


_finder = None


def take_finder():
    global _finder

    finder = _finder
    if finder is not None:
        _finder = None
    return finder


def _bootstrap(data, name):
    if name in sys.modules:
        raise RuntimeError('{} already in sys.modules'.format(name))

    tmp_loader = _RemoteLoader(*data[name])
    spec = importlib.util.spec_from_loader(name, tmp_loader)
    module = _module_from_spec(spec)

    sys.modules[name] = module
    tmp_loader.exec_module(module)
    loaders = dict(
        (fullname, module._RemoteLoader(*args))
        for fullname, args in data.items()
        if fullname != name
    )
    module.__loader__ = spec.loader = loaders[name] = module._RemoteLoader(*tmp_loader.deconstruct())
    finder = module._RemoteFinder(loaders)
    sys.meta_path.append(finder)

    sys.modules['__main__'] = _LazyMain(finder)

    module._finder = finder
