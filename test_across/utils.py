"""
Test helpers.
"""

import threading
import io
import subprocess
import pickle
import tempfile
import os.path
import atexit
import shutil
import unittest
import sys
import socket

import across
import across.channels


class _MemoryPipe:
    def __init__(self):
        self.__lock = threading.Lock()
        self.__send_condition = threading.Condition(self.__lock)
        self.__recv_condition = threading.Condition(self.__lock)
        self.__send_buffer, self.__send_size = None, None
        self.__recv_buffer, self.__recv_size = None, None
        self.__closed = False

    def send(self, buffer):
        with self.__lock:
            if self.__send_buffer is not None or self.__send_size is not None:
                raise AssertionError('send already in progress')
            if self.__closed:
                raise ValueError
            if self.__recv_buffer is not None:
                size = min(len(buffer), len(self.__recv_buffer))
                self.__recv_buffer[:size] = buffer[:size]
                self.__recv_buffer, self.__recv_size = None, size
                self.__recv_condition.notify()
                return size
            self.__send_buffer = buffer
            while self.__send_size is None and not self.__closed:
                self.__send_condition.wait()
            if self.__send_size is None:
                raise ValueError
            size, self.__send_size = self.__send_size, None
            return size

    def recv_into(self, buffer):
        with self.__lock:
            if self.__recv_buffer is not None or self.__recv_size is not None:
                raise AssertionError('recv already in progress')
            if self.__closed:
                raise ValueError
            if self.__send_buffer is not None:
                size = min(len(buffer), len(self.__send_buffer))
                buffer[:size] = self.__send_buffer[:size]
                self.__send_buffer, self.__send_size = None, size
                self.__send_condition.notify()
                return size
            self.__recv_buffer = buffer
            while self.__recv_size is None and not self.__closed:
                self.__recv_condition.wait()
            if self.__recv_size is None:
                raise ValueError
            size, self.__recv_size = self.__recv_size, None
            return size

    def close(self):
        with self.__lock:
            self.__closed = True
            self.__send_buffer, self.__recv_buffer = None, None
            self.__send_condition.notify()
            self.__recv_condition.notify()


class _MemoryChannel(across.channels.Channel):
    def __init__(self, stdin, stdout):
        self.__stdin = stdin
        self.__stdout = stdout

    def recv_into(self, buffer):
        return self.__stdin.recv_into(buffer)

    def send(self, buffer):
        return self.__stdout.send(buffer)

    def cancel(self):
        self.__stdin.close()
        self.__stdout.close()


def make_channel_pair():
    pipe1, pipe2 = _MemoryPipe(), _MemoryPipe()
    return _MemoryChannel(pipe1, pipe2), _MemoryChannel(pipe2, pipe1)


# Channel with bundled remote connection.
class ConnectionChannel(_MemoryChannel):
    def __init__(self):
        pipe1, pipe2 = _MemoryPipe(), _MemoryPipe()
        super().__init__(pipe1, pipe2)
        self.__conn = across.Connection(_MemoryChannel(pipe2, pipe1))

    def close(self):
        self.__conn.close()


def make_connection():
    return across.Connection(ConnectionChannel())


_box_counter = 0
_box_counter_lock = threading.Lock()
_box_values = {}


class Box(object):
    def __init__(self, value=None):
        self.value = value

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)

    def __getstate__(self):
        global _box_counter
        with _box_counter_lock:
            key = _box_counter
            _box_counter += 1
        _box_values[key] = self.value
        return key

    def __setstate__(self, key):
        self.value = _box_values.pop(key)


class _ParThread(threading.Thread):
    def __init__(self, func):
        super(_ParThread, self).__init__()
        self.__func = func
        self.__value = None
        self.__error = None
        self.start()

    def get_value(self):
        if self.__error:
            try:
                raise self.__error
            finally:
                self.__error = None
        return self.__value

    def run(self):
        try:
            self.__value = self.__func()
        except Exception as error:
            self.__error = error


def par(*funcs):
    threads = [_ParThread(func) for func in funcs]
    for thread in threads:
        thread.join()
    return tuple(thread.get_value() for thread in threads)


class StderrCollector:
    def __init__(self):
        self.__stderr = io.StringIO()
        self.__orig_stderr = None

    def getvalue(self):
        return self.__stderr.getvalue()

    def __enter__(self):
        self.__orig_stderr = sys.stderr
        sys.stderr = self.__stderr
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stderr = self.__orig_stderr


def _call_process(func, args, kwargs, **popen_args):
    process = subprocess.Popen(
        [sys.executable, '-c', """
import pickle, sys
func, args, kwargs = pickle.load(sys.stdin.buffer)
try:
    result = True, func(*args, **kwargs)
except Exception as exc:
    result = False, exc
pickle.dump(result, sys.stdout.buffer)"""],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        **popen_args,
    )
    stdout, stderr = process.communicate(pickle.dumps((func, args, kwargs)))
    if stderr is not None:
        stderr = stderr.decode()
    success, value = pickle.loads(stdout)
    if success:
        return value, stderr
    else:
        raise value


def call_process(func, *args, **kwargs):
    return _call_process(func, args, kwargs)[0]


def call_process_with_stderr(func, *args, **kwargs):
    return _call_process(func, args, kwargs, stderr=subprocess.PIPE)


_tmpdir = None
_tmp_counter = 0


def mktemp():
    global _tmpdir, _tmp_counter

    if _tmpdir is None:
        _tmpdir = tempfile.mkdtemp()

    _tmp_counter += 1

    return os.path.join(_tmpdir, str(_tmp_counter))


def _cleanup_tmpdir():
    if _tmpdir:
        shutil.rmtree(_tmpdir)


atexit.register(_cleanup_tmpdir)


class PackageTestLoader:
    def __init__(self, modname):
        self._mods = []
        mod = sys.modules[modname]
        if not hasattr(mod, '__file__') or not os.path.isfile(mod.__file__):
            # module is probably being imported by across._importer
            return
        pkg_dir = os.path.dirname(mod.__file__)
        for filename in sorted(os.listdir(pkg_dir)):
            if not filename.startswith('_') and filename.endswith('.py'):
                modbase = os.path.splitext(filename)[0]
                submodname = '{}.{}'.format(modname, modbase)
                __import__(submodname)
                submod = sys.modules[submodname]
                self._mods.append(submod)

    def __call__(self, loader, tests, pattern):
        suite = unittest.TestSuite()
        for mod in self._mods:
            suite.addTests(loader.loadTestsFromModule(mod))
        return suite


# Printed by logging module to stderr when internal error occurs.
logging_error_marker = '--- Logging error ---'

localhost = '127.0.0.1'
localhost_ipv6 = '::1'

windows = sys.platform == 'win32'

skip_if_no_unix_sockets = unittest.skipUnless(hasattr(socket, 'AF_UNIX'), 'Unix domain sockets are not available')
