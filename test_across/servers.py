import threading
import socket
import select
import unittest.mock
import traceback
import os
import time
import atexit

from across import Connection, DisconnectError, set_debug_level, Options
from across.servers import (
    run_tcp, run_unix, LocalConnectionHandler, ProcessConnectionHandler, BIOSConnectionHandler)
from across._utils import get_debug_level

from .utils import (
    mktemp, localhost, localhost_ipv6, anyaddr_ipv6, windows, skip_if_no_unix_sockets, call_process_with_stderr,
    logging_error_marker, InheritableStderrCollector)


def make_tcp_socket_pair():
    server = socket.socket()
    server.bind((localhost, 0))
    server.listen(1)
    first_socket = socket.socket()
    first_socket.connect(server.getsockname())
    second_socket = server.accept()[0]
    server.close()
    return first_socket, second_socket


if windows:
    class Interrupter:
        def __init__(self):
            self.__in_pipe, self.__out_pipe = make_tcp_socket_pair()

        def fileno(self):
            return self.__in_pipe.fileno()

        def interrupt(self):
            self.__out_pipe.send(b'\0')

        def close(self):
            self.__in_pipe.close()
            self.__out_pipe.close()
else:
    class Interrupter:
        def __init__(self):
            self.__pipe = os.pipe()

        def fileno(self):
            return self.__pipe[0]

        def interrupt(self):
            os.write(self.__pipe[1], b'\0')

        def close(self):
            os.close(self.__pipe[0])
            os.close(self.__pipe[1])


class SocketForServer:
    def __init__(self):
        self.__sock = None
        self.__interrupter = Interrupter()
        self.__listening = threading.Event()

    def __getattr__(self, item):
        return getattr(self.__sock, item)

    def open(self, family):
        assert self.__sock is None
        self.__sock = socket.socket(family)
        return self

    def listen(self, backlog):
        self.__sock.listen(backlog)
        self.__listening.set()

    def accept(self):
        readable_fds, _, _ = select.select([self.__sock, self.__interrupter], [], [])
        if self.__interrupter in readable_fds:
            self.__interrupter.close()
            raise KeyboardInterrupt

        return self.__sock.accept()

    def wait_listen(self, timeout):
        return self.__listening.wait(timeout)

    def interrupt(self):
        self.__interrupter.interrupt()


class ServerWorker:
    def __init__(self, func, *args, **kwargs):
        self.__func = func
        self.__args = args
        self.__kwargs = kwargs
        self.__thread = threading.Thread(target=self.__thread_func, daemon=True)
        self.__error = None
        self.socket = SocketForServer()

        with unittest.mock.patch('across.servers._open_socket', new=self.socket.open):
            self.__thread.start()
            while not self.socket.wait_listen(0.01):
                if self.__error:
                    raise self.__error
        self.address = self.socket.getsockname()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.stop()

    def __thread_func(self):
        try:
            self.__func(*self.__args, **self.__kwargs)
        except BaseException as error:
            traceback.print_exc()
            self.__error = error

    def stop(self):
        self.socket.interrupt()
        self.__thread.join()
        if self.__error:
            raise self.__error


def add(left, right):
    return left + right


class ConnectionHandlerTest(unittest.TestCase):
    def make_handler(self, **kwargs):
        raise NotImplementedError

    def make_connection(self, handler):
        sock1, sock2 = make_tcp_socket_pair()
        conn = Connection.from_socket(sock1)
        handler.handle_socket(sock2)
        return conn

    def make_handler_and_connection(self):
        handler = self.make_handler()
        conn = self.make_connection(handler)
        return handler, conn

    def test_close_with_running_connection(self):
        handler, conn = self.make_handler_and_connection()
        self.assertEqual(conn.call(add, 1, 2), 3)
        handler.close()
        if windows:
            try:
                conn.close()
            except OSError:  # Connection reset by peer
                pass
        else:
            conn.close()


    def test_cancel_with_running_connection(self):
        handler, conn = self.make_handler_and_connection()
        self.assertEqual(conn.call(add, 1, 2), 3)
        handler.cancel()
        handler.close()
        self.assertRaises(Exception, conn.close)

    def test_handle_after_cancel(self):
        sock = socket.socket()
        with self.make_handler() as handler:
            handler.cancel()
            self.assertRaises(ValueError, handler.handle_socket, sock)
        sock.close()

    def test_handle_after_close(self):
        sock = socket.socket()
        handler = self.make_handler()
        handler.close()
        self.assertRaises(ValueError, handler.handle_socket, sock)
        sock.close()

    def test_handshake_timeout(self):
        sock1, sock2 = make_tcp_socket_pair()
        # In process-based handlers, child process prints timeout exception on stderr.
        with InheritableStderrCollector():
            with self.make_handler(options=Options(timeout=0.01)) as handler:
                handler.handle_socket(sock2)
                # Wait for other side to close the connection.
                try:
                    while sock1.recv(1024) != b'':
                        pass
                except OSError:  # Connection reset by peer on Windows
                    pass
        sock1.close()


def hang_on_process_shutdown():
    def func():
        while True:
            try:
                time.sleep(60)
            except KeyboardInterrupt:
                pass
    atexit.register(func)


class ProcessBasedConnectionHandlerTest(ConnectionHandlerTest):
    @unittest.mock.patch('across.servers._get_process_close_timeout', new=lambda options: 0.01)
    def test_close_timeout(self):
        handler, conn = self.make_handler_and_connection()
        conn.call(hang_on_process_shutdown)
        handler.close()
        try:
            conn.close()
        except Exception:
            pass


class LocalConnectionHandlerTest(ConnectionHandlerTest):
    def make_handler(self, **kwargs):
        return LocalConnectionHandler(**kwargs)

    def test_accepting_exported_modules_is_forbidden(self):
        self.assertRaises(ValueError, LocalConnectionHandler, options=Options(accept_exported_modules=True))


class ProcessConnectionHandlerTest(ProcessBasedConnectionHandlerTest):
    def make_handler(self, **kwargs):
        return ProcessConnectionHandler(**kwargs)


class BIOSConnectionHandlerTest(ProcessBasedConnectionHandlerTest):
    def make_handler(self, **kwargs):
        return BIOSConnectionHandler(**kwargs)


# Remove abstract test classes
del ConnectionHandlerTest
del ProcessBasedConnectionHandlerTest


class ServerTest(unittest.TestCase):
    def test_tcp(self):
        with ServerWorker(run_tcp, localhost, 0) as worker:
            with Connection.from_tcp(*worker.address) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)

    def test_tcp_ipv6(self):
        with ServerWorker(run_tcp, localhost_ipv6, 0) as worker:
            with Connection.from_tcp(*worker.address[:2]) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)

    def test_tcp_dual_stack(self):
        with ServerWorker(run_tcp, anyaddr_ipv6, 0) as worker:
            with Connection.from_tcp(localhost, worker.address[1]) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)

    def test_multiple_connections(self):
        with ServerWorker(run_tcp, localhost, 0) as worker:
            num_conns = 5

            conns = [Connection.from_tcp(*worker.address) for _ in range(num_conns)]

            for i, conn in enumerate(conns):
                self.assertEqual(conn.call(add, i, 1), i + 1)

            for conn in conns:
                conn.close()

    def test_process_handler(self):
        handler = ProcessConnectionHandler()
        # Child process may be interrupted during interpreter shutdown, causing various errors to be
        # printed on stderr. Let's suppress them.
        with InheritableStderrCollector():
            with ServerWorker(run_tcp, localhost, 0, handler=handler) as worker:
                with Connection.from_tcp(*worker.address) as conn:
                    self.assertEqual(conn.call(add, 1, 2), 3)
                    self.assertNotEqual(conn.call(os.getpid), os.getpid())

    def test_bios_handler(self):
        handler = BIOSConnectionHandler()
        # Child process may be interrupted during interpreter shutdown, causing various errors to be
        # printed on stderr. Let's suppress them.
        with InheritableStderrCollector():
            with ServerWorker(run_tcp, localhost, 0, handler=handler) as worker:
                with Connection.from_tcp(*worker.address) as conn:
                    self.assertEqual(conn.call(add, 1, 2), 3)
                    self.assertNotEqual(conn.call(os.getpid), os.getpid())

    def test_stopping_server_with_local_handler_and_active_connections(self):
        self.__run_stopping_test(LocalConnectionHandler())

    def test_stopping_server_with_process_handler_and_active_connections(self):
        self.__run_stopping_test(ProcessConnectionHandler())

    def test_stopping_server_with_bios_handler_and_active_connections(self):
        self.__run_stopping_test(BIOSConnectionHandler())

    def __run_stopping_test(self, handler):
        with ServerWorker(run_tcp, localhost, 0, handler=handler) as worker:
            conn = Connection.from_tcp(*worker.address)
            self.assertEqual(conn.call(add, 1, 2), 3)
        self.assertRaises(DisconnectError, conn.call, add, 1, 2)
        try:
            conn.close()
        except OSError:  # Connection reset by peer on Windows
            pass


@skip_if_no_unix_sockets
class UnixServerTest(unittest.TestCase):
    def test_unix(self):
        path = mktemp()
        with ServerWorker(run_unix, path):
            with Connection.from_unix(path) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)


class DebugTest(unittest.TestCase):
    def test_process_handler_forwards_debug_level(self):
        _, stderr = call_process_with_stderr(_set_debug_level_and_connect)
        self.assertNotIn(logging_error_marker, stderr)


def _set_debug_level_and_connect():
    set_debug_level(10)
    handler = ProcessConnectionHandler()
    with ServerWorker(run_tcp, localhost, 0, handler=handler) as worker:
        with Connection.from_tcp(*worker.address) as conn:
            if conn.call(get_debug_level) != 10:
                raise AssertionError
