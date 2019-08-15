import threading
import socket
import subprocess
import sys
import pickle
import base64
import os.path
import signal
import time

from ._utils import ignore_exception_at, logger as _logger, get_debug_level, set_debug_level
from . import _get_bios_superblock, Connection, _sanitize_options


_windows = (sys.platform == 'win32')


class ConnectionHandler:
    def handle_socket(self, sock):
        raise NotImplementedError

    def cancel(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class LocalConnectionHandler(ConnectionHandler):
    def __init__(self, *, options=None):
        self.__lock = threading.Lock()
        self.__unclosed_conns = set()
        self.__stopped_conn = None
        self.__closed = False
        self.__cancelled = False
        self.__options = _sanitize_options(options)
        if self.__options.accept_exported_modules:
            raise ValueError('Cannot import modules over multiple connections')

    def handle_socket(self, sock):
        with self.__lock:
            if self.__closed:
                raise ValueError('Connection handler is closed')
            if self.__cancelled:
                raise ValueError('Connection handler is cancelled')
            conn = Connection.from_socket(sock, options=self.__options, on_stopped=self.__connection_stopped)
            self.__unclosed_conns.add(conn)

    def __close_stopped_connection_locked(self):
        self.__unclosed_conns.remove(self.__stopped_conn)
        self.__close_conn(self.__stopped_conn)
        self.__stopped_conn = None

    def __connection_stopped(self, conn):
        with self.__lock:
            if self.__closed:
                return
            if self.__stopped_conn:
                self.__close_stopped_connection_locked()
            self.__stopped_conn = conn

    def __close_conn(self, conn):
        try:
            conn.close()
        # Connection.close() may raise all different exception types.
        except Exception:
            ignore_exception_at(conn.close)

    def cancel(self):
        with self.__lock:
            self.__cancelled = True
            for conn in self.__unclosed_conns:
                conn.cancel()

    def close(self):
        with self.__lock:
            self.__closed = True
            if self.__stopped_conn:
                self.__close_stopped_connection_locked()
            conns = self.__unclosed_conns
            self.__unclosed_conns = []
        for conn in conns:
            self.__close_conn(conn)


_serve_arg = '_serve'


def _get_process_close_timeout(options):  # patched by tests
    return options.timeout


class _ProcessConnectionHandlerBase(ConnectionHandler):
    def __init__(self, *, options=None):
        self._procs = set()
        self._cancelled = False
        self._closed = False
        self._options = _sanitize_options(options)

    def handle_socket(self, sock):
        if self._closed:
            raise ValueError('Connection handler is closed')
        if self._cancelled:
            raise ValueError('Connection handler is cancelled')
        proc = self._create_proc(sock)
        _logger.debug('Started child process, pid=%r', proc.pid)
        self._procs.add(proc)
        self._collect_procs()

    if _windows:
        def _create_proc(self, sock):
            proc = subprocess.Popen(
                self._get_base_args(),
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
            )
            proc.stdin.write(sock.share(proc.pid))
            proc.stdin.close()
            sock.close()
            return proc

        # There's CTRL_C_EVENT on Windows, but it's restricted to processes running in a console window.
        # Since there's no better alternative, let's just kill the process.
        _interrupt_signal = signal.SIGTERM
    else:
        def _create_proc(self, sock):
            proc = subprocess.Popen(
                self._get_base_args() + [str(int(sock.family))],
                stdin=sock.fileno(),
                stdout=subprocess.DEVNULL,
            )
            sock.close()
            return proc

        _interrupt_signal = signal.SIGINT

    def _collect_procs(self):
        for proc in self._procs.copy():
            if proc.poll() is not None:
                _logger.debug('Joined process %r', proc.pid)
                self._procs.remove(proc)

    def _get_base_args(self):
        raise NotImplementedError

    def _interrupt_proc(self, proc):
        if self._cancelled:
            return
        if proc.poll() is None:
            _logger.debug('Interrupting process %r', proc.pid)
            try:
                proc.send_signal(self._interrupt_signal)
            except OSError:  # process already exited
                pass

    def _kill_proc(self, proc):
        if self._cancelled:
            return
        if proc.poll() is None:
            _logger.debug('Killing process %r', proc.pid)
            try:
                proc.kill()
            except OSError:  # process already exited
                pass

    def _wait_proc(self, proc, timeout):
        if timeout <= 0.0:
            return
        try:
            proc.wait(timeout)
        except subprocess.TimeoutExpired:
            pass

    def cancel(self):
        for proc in self._procs:
            self._kill_proc(proc)
        self._cancelled = True

    def close(self):
        self._closed = True
        for proc in self._procs:
            self._interrupt_proc(proc)
        deadline = time.time() + _get_process_close_timeout(self._options)
        for proc in self._procs:
            _logger.debug('Joining process %r', proc.pid)
            self._wait_proc(proc, deadline - time.time())
            self._kill_proc(proc)
            proc.wait()
            _logger.debug('Process joined')


class ProcessConnectionHandler(_ProcessConnectionHandlerBase):
    def _get_base_args(self):
        data = base64.b64encode(pickle.dumps((get_debug_level(), self._options))).decode('ascii')
        return [sys.executable, '-m', __name__, _serve_arg, data]


if _windows:
    _socket_bios = r"""import socket,sys
sock = socket.fromshare(sys.stdin.buffer.read())
"""
else:
    _socket_bios = r"""import os,sys,socket
sock = socket.fromfd(0, int(sys.argv[2]), socket.SOCK_STREAM)
devnull_fd = os.open(os.devnull, os.O_RDONLY)
os.dup2(devnull_fd, 0)
os.close(devnull_fd)
"""

_socket_bios += r"""import struct
sock.settimeout(float(sys.argv[1]))
sock.sendall({superblock!r})
def recvall(size):
    buf = b''
    while len(buf) < size:
        buf += sock.recv(size - len(buf)) or sys.exit(1)
    return buf
ACROSS = 'socket', sock
try:
    exec(recvall(struct.unpack('>I', recvall(20)[-4:])[0]))
except KeyboardInterrupt:
    raise SystemExit
""".format(superblock=_get_bios_superblock())


class BIOSConnectionHandler(_ProcessConnectionHandlerBase):
    def _get_base_args(self):
        return [sys.executable, '-c', _socket_bios, str(self._options.timeout)]


# IPPROTO_IPV6 is missing on Windows and Python < 3.8
try:
    _IPPROTO_IPV6 = socket.IPPROTO_IPV6
except AttributeError:
    if _windows:
        # https://github.com/mirror/mingw-w64/blob/16151c441e89081fd398270bb888511ebef6fb35/
        # mingw-w64-headers/include/winsock2.h
        _IPPROTO_IPV6 = 41
    else:
        raise


def _tune_server_socket(sock):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if sock.family == socket.AF_INET6:
        sock.setsockopt(_IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)


_open_socket = socket.socket  # patched by tests


def _run_server(family, address, handler):
    if handler is None:
        handler = LocalConnectionHandler()
    sock = _open_socket(family)
    _tune_server_socket(sock)
    sock.bind(address)
    _logger.info('Listening on %r', address)
    sock.listen(socket.SOMAXCONN)
    try:
        while True:
            client_sock, client_addr = sock.accept()
            _logger.info('Accepted connection from %r', client_addr)
            handler.handle_socket(client_sock)
    except KeyboardInterrupt:
        _logger.info('Received interrupt')
    sock.close()
    handler.close()


def run_tcp(host, port, *, handler=None):
    addrinfo = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    if not addrinfo:
        # C getaddrinfo() returns one or more items, so this should hopefully never happen
        raise RuntimeError('getaddrinfo returned empty list')
    family, _, _, _, address = addrinfo[0]
    _run_server(family, address, handler)


def run_unix(path, *, handler=None):
    if not hasattr(socket, 'AF_UNIX'):
        raise RuntimeError('Unix domain sockets are not available')
    if os.path.exists(path):
        os.unlink(path)
    _run_server(socket.AF_UNIX, path, handler)
    os.unlink(path)


def main():
    # safeguard in case someone accidentally runs this module
    if len(sys.argv) < 2 or sys.argv[1] != _serve_arg:
        return

    debug_level, options = pickle.loads(base64.b64decode(sys.argv[2]))
    set_debug_level(debug_level)
    if _windows:
        sock = socket.fromshare(sys.stdin.buffer.read())
    else:
        family = int(sys.argv[3])
        sock = socket.fromfd(0, family, socket.SOCK_STREAM)
        devnull_fd = os.open(os.devnull, os.O_RDONLY)
        os.dup2(devnull_fd, 0)
        os.close(devnull_fd)
    try:
        with Connection.from_socket(sock, options=options) as conn:
            conn.wait()
    except KeyboardInterrupt:
        raise SystemExit


if __name__ == '__main__':
    main()
