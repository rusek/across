import threading
import socket
import select
import unittest.mock
import traceback
import os

import across.servers

from .utils import mktemp, localhost, localhost_ipv6


class SocketForServer:
    def __init__(self):
        self.__sock = None
        self.__listening = threading.Event()
        self.__interrupt_pipe = os.pipe()

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
        readable_fds, _, _ = select.select([self.__sock, self.__interrupt_pipe[0]], [], [])
        if self.__interrupt_pipe[0] in readable_fds:
            os.close(self.__interrupt_pipe[0])
            os.close(self.__interrupt_pipe[1])
            raise KeyboardInterrupt

        return self.__sock.accept()

    def wait_listen(self, timeout):
        return self.__listening.wait(timeout)

    def interrupt(self):
        os.write(self.__interrupt_pipe[1], b'\0')


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


class ServerTest(unittest.TestCase):
    def test_tcp(self):
        with ServerWorker(across.servers.run_tcp, localhost, 0) as worker:
            with across.Connection.from_tcp(*worker.address) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)

    def test_tcp_ipv6(self):
        with ServerWorker(across.servers.run_tcp, localhost_ipv6, 0) as worker:
            with across.Connection.from_tcp(*worker.address[:2]) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)

    def test_unix(self):
        path = mktemp()
        with ServerWorker(across.servers.run_unix, path):
            with across.Connection.from_unix(path) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)

    def test_multiple_connections(self):
        with ServerWorker(across.servers.run_tcp, localhost, 0) as worker:
            num_conns = 5

            conns = [across.Connection.from_tcp(*worker.address) for _ in range(num_conns)]

            for i, conn in enumerate(conns):
                self.assertEqual(conn.call(add, i, 1), i + 1)

            for conn in conns:
                conn.close()

    def test_process_handler(self):
        handler = across.servers.ProcessConnectionHandler()
        with ServerWorker(across.servers.run_tcp, localhost, 0, handler=handler) as worker:
            with across.Connection.from_tcp(*worker.address) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)
                self.assertNotEqual(conn.call(os.getpid), os.getpid())

    def test_bootstrapping_handler(self):
        handler = across.servers.BootstrappingConnectionHandler()
        with ServerWorker(across.servers.run_tcp, localhost, 0, handler=handler) as worker:
            with across.Connection.from_tcp(*worker.address) as conn:
                self.assertEqual(conn.call(add, 1, 2), 3)
                self.assertNotEqual(conn.call(os.getpid), os.getpid())

    def test_stopping_server_with_local_handler_and_active_connections(self):
        self.__run_stopping_test(across.servers.LocalConnectionHandler())

    def test_stopping_server_with_process_handler_and_active_connections(self):
        self.__run_stopping_test(across.servers.ProcessConnectionHandler())

    def test_stopping_server_with_bootstrapping_handler_and_active_connections(self):
        self.__run_stopping_test(across.servers.BootstrappingConnectionHandler())

    def __run_stopping_test(self, handler):
        with ServerWorker(across.servers.run_tcp, localhost, 0, handler=handler) as worker:
            conn = across.Connection.from_tcp(*worker.address)
            self.assertEqual(conn.call(add, 1, 2), 3)
        self.assertRaises(across.DisconnectError, conn.call, add, 1, 2)
        self.assertRaises(Exception, conn.close)
