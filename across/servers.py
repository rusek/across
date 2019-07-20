import threading
import socket
import subprocess
import sys
import os.path

from .utils import ignore_exception_at, logger as _logger, get_debug_level, set_debug_level
from . import _get_bios_superblock
import across.channels


_windows = (sys.platform == 'win32')


class ConnectionHandler:
    def handle_socket(self, sock):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class LocalConnectionHandler(ConnectionHandler):
    def __init__(self):
        self.__lock = threading.Lock()
        self.__unclosed_conns = set()
        self.__stopped_conn = None
        self.__stopped_conn_condition = threading.Condition(self.__lock)
        self.__closing = False

    def handle_socket(self, sock):
        with self.__lock:
            if self.__closing:
                sock.close()
                return
            channel = across.channels.SocketChannel(sock=sock)
            conn = across.Connection(channel, on_stopped=self.__connection_stopped)
            self.__unclosed_conns.add(conn)

    def __close_stopped_connection_locked(self):
        self.__unclosed_conns.remove(self.__stopped_conn)
        try:
            self.__stopped_conn.close()
        except BaseException:
            # Ignore exceptions after cancelling connections
            if not self.__closing:
                ignore_exception_at(self.__stopped_conn.close)
        self.__stopped_conn = None

    def __connection_stopped(self, conn):
        with self.__lock:
            if self.__stopped_conn:
                self.__close_stopped_connection_locked()
            self.__stopped_conn = conn
            self.__stopped_conn_condition.notify()

    def close(self):
        with self.__lock:
            self.__closing = True
            for conn in self.__unclosed_conns:
                conn.cancel()
            while self.__unclosed_conns:
                if self.__stopped_conn:
                    self.__close_stopped_connection_locked()
                else:
                    self.__stopped_conn_condition.wait()


_serve_arg = '_serve'


class ProcessConnectionHandler(ConnectionHandler):
    def __init__(self):
        self._procs = set()

    def handle_socket(self, sock):
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
    else:
        def _create_proc(self, sock):
            proc = subprocess.Popen(
                self._get_base_args() + [str(int(sock.family))],
                stdin=sock.fileno(),
                stdout=subprocess.DEVNULL,
            )
            sock.close()
            return proc

    def _collect_procs(self):
        for proc in self._procs.copy():
            if proc.poll() is not None:
                _logger.debug('Joined process %r', proc.pid)
                self._procs.remove(proc)

    def _get_base_args(self):
        return [sys.executable, '-m', __name__, _serve_arg, str(get_debug_level())]

    def close(self):
        for proc in self._procs:
            if proc.poll() is None:
                _logger.debug('Killing process %r', proc.pid)
                try:
                    proc.terminate()
                except OSError:  # process already terminated
                    pass
        for proc in self._procs:
            _logger.debug('Joining process %r', proc.pid)
            proc.wait()
            _logger.debug('Process joined')


if _windows:
    _socket_bios = r"""import socket,sys
sock = socket.fromshare(sys.stdin.buffer.read())
"""
else:
    _socket_bios = r"""import os,sys,socket
sock = socket.fromfd(0, int(sys.argv[1]), socket.SOCK_STREAM)
devnull_fd = os.open(os.devnull, os.O_RDONLY)
os.dup2(devnull_fd, 0)
os.close(devnull_fd)
"""

_socket_bios += r"""import struct
sock.sendall({superblock!r})
def recvall(size):
    buf = b''
    while len(buf) < size:
        buf += sock.recv(size - len(buf)) or sys.exit(1)
    return buf
ACROSS = 'socket', sock
exec(recvall(struct.unpack('>I', recvall(20)[-4:])[0]))
""".format(superblock=_get_bios_superblock())


class BootstrappingConnectionHandler(ProcessConnectionHandler):
    def _get_base_args(self):
        return [sys.executable, '-c', _socket_bios]


def _tune_server_socket(sock):
    if sock.family in (socket.AF_INET, socket.AF_INET6):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


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

    set_debug_level(int(sys.argv[2]))
    if _windows:
        sock = socket.fromshare(sys.stdin.buffer.read())
    else:
        family = int(sys.argv[3])
        sock = socket.fromfd(0, family, socket.SOCK_STREAM)
        devnull_fd = os.open(os.devnull, os.O_RDONLY)
        os.dup2(devnull_fd, 0)
        os.close(devnull_fd)
    with across.Connection(across.channels.SocketChannel(sock=sock)) as conn:
        conn.wait()


if __name__ == '__main__':
    main()
