import sys
import importlib
import importlib.util
import types
import sysconfig
import os.path


def _get_loader(fullname):
    try:
        spec = importlib.util.find_spec(fullname)
    except ImportError:
        return None
    if spec is None:
        return None
    if spec.loader is None:
        raise ValueError('%r.loader is not set' % (spec, ))
    return spec.loader


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
    loader = _get_loader(fullname)
    # this (checking if module exists) must go before _is_stdlib_module
    if loader is None:
        return None
    if _is_stdlib_module(fullname):
        return None
    get_source = getattr(loader, 'get_source', None)
    if get_source is None:
        raise TypeError('Loader %r for module %s does not implement get_source method' % (loader, fullname))
    source = get_source(fullname)
    if source is None:
        raise ValueError('Source is not available for loader %r and module %r' % (loader, fullname))
    is_package = getattr(loader, 'is_package', None)
    if is_package is None:
        raise TypeError('Loader %r for module %s does not implement is_package method' % (loader, fullname))
    package = is_package(fullname)
    return _RemoteLoader(source, package)


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
    def __init__(self, source, package, code=None):
        self.__source = source
        self.__package = package
        self.__code = code

    def get_code(self, fullname=None):
        if self.__code is None:
            self.__code = compile(self.get_source(), '<string>', 'exec', dont_inherit=True)
        return self.__code

    def get_source(self, fullname=None):
        return self.__source

    def is_package(self, fullname=None):
        return self.__package

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        exec(self.get_code(), module.__dict__)

    def deconstruct(self):
        return self.__source, self.__package, self.__code


_minimal_modules = (
    'across',
    'across._importer',
)


def get_bootstrap_line():
    modules = {}
    for fullname in _minimal_modules:
        loader = _get_remote_loader(fullname)
        if loader is None:
            raise ImportError('%s not found' % (fullname, ))
        modules[fullname] = loader.deconstruct()
    return """data, __name__ = %r, %r; exec(data[__name__][0]); _bootstrap(data)\n""" % (modules, __name__)


def _bootstrap(data):
    if __name__ in sys.modules:
        raise RuntimeError('%s already in sys.modules' % (__name__, ))

    tmp_loader = _RemoteLoader(*data[__name__])
    spec = importlib.util.spec_from_loader(__name__, tmp_loader)
    module = types.ModuleType(__name__)
    module.__spec__ = spec

    sys.modules[__name__] = module
    tmp_loader.exec_module(module)
    loaders = dict(
        (fullname, module._RemoteLoader(*args))
        for fullname, args in data.items()
        if fullname != __name__
    )
    loader = loaders[__name__] = module._RemoteLoader(*tmp_loader.deconstruct())
    module.__loader__ = loader
    module.__spec__.loader = loader
    finder = module._RemoteFinder(loaders)
    sys.meta_path.append(finder)

    from across import _BootstrappedConnection, PipeChannel
    channel = PipeChannel(sys.stdin.buffer, sys.stdout.buffer)
    with _BootstrappedConnection(channel, finder) as conn:
        conn.wait()
