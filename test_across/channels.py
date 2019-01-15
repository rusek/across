from across.channels import SocketChannel, Channel
from test_across.utils import par, mktemp
import unittest
import socket
import time
import os.path


def get_open_fds():
    if os.path.isdir('/proc/self/fd'):
        return set(os.listdir('/proc/self/fd'))
    else:
        return set()


class Server:
    def create_client(self):
        raise NotImplementedError

    def listen(self):
        raise NotImplementedError

    def accept(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class BasicSocketServer(Server):
    def __init__(self, family, address):
        self.__sock = socket.socket(family)
        self.__sock.bind(address)

    def create_client(self):
        return SocketChannel(family=self.__sock.family, address=self.__sock.getsockname())

    def listen(self):
        self.__sock.listen(1)

    def accept(self):
        return BasicSocketChannel(self.__sock.accept()[0])

    def close(self):
        self.__sock.close()


class BasicSocketChannel(Channel):
    def __init__(self, sock):
        self.sock = sock

    def send(self, data):
        return self.sock.send(data)

    def recv(self, size):
        return self.sock.recv(size)

    def close(self):
        return self.sock.close()


def sendall(obj, data):
    while data:
        data = data[obj.send(data):]


def recvall(obj, size):
    data = b''
    while size:
        ret = obj.recv(size)
        if not ret:
            return data
        size -= len(ret)
        data += ret

    return data


class ChannelTest(unittest.TestCase):
    def setUp(self):
        self.initial_fds = get_open_fds()

    def tearDown(self):
        self.assertEqual(self.initial_fds, get_open_fds())

    def create_server(self):
        raise NotImplementedError

    def verify_exception(self, exc):
        self.assertIsInstance(exc, (OSError, ValueError))

    def connect_pair(self):
        server = self.create_server()
        server.listen()
        chan = server.create_client()
        chan.connect()
        rchan = server.accept()
        server.close()
        return chan, rchan

    def test_connect(self):
        chan, rchan = self.connect_pair()
        rchan.close()
        chan.close()

    def test_connect_error(self):
        server = self.create_server()
        chan = server.create_client()
        try:
            chan.connect()
        except Exception as exc:
            self.verify_exception(exc)
        else:
            self.fail('Exception not raised')
        server.close()

    def test_graceful_close(self):
        chan, rchan = self.connect_pair()

        def close_rchan():
            rchan.recv(1)  # recv EOF
            rchan.close()

        par(chan.close, close_rchan)

    def test_singlebyte_send(self):
        chan, rchan = self.connect_pair()
        self.assertEqual(chan.send(b'x'), 1)
        self.assertEqual(rchan.recv(1), b'x')
        rchan.close()
        chan.close()

    def test_singlebyte_recv(self):
        chan, rchan = self.connect_pair()
        rchan.send(b'x')
        self.assertEqual(chan.recv(1), b'x')
        rchan.close()
        chan.close()

    def test_multibyte_send(self):
        chan, rchan = self.connect_pair()

        data = b'y' * 1024 * 1024
        _, recv_data = par(lambda: sendall(chan, data), lambda: recvall(rchan, len(data)))
        self.assertEqual(data, recv_data)

        rchan.close()
        chan.close()

    def test_multibyte_recv(self):
        chan, rchan = self.connect_pair()

        data = b'y' * 1024 * 1024
        _, recv_data = par(lambda: sendall(rchan, data), lambda: recvall(chan, len(data)))
        self.assertEqual(data, recv_data)

        rchan.close()
        chan.close()

    def test_cancel_send(self):
        chan, rchan = self.connect_pair()

        def send():
            try:
                while True:
                    chan.send(b'x' * 1024 * 64)
            except Exception as exc:
                self.verify_exception(exc)

        def cancel():
            time.sleep(0.01)
            chan.cancel()

        par(send, cancel)

        rchan.close()
        chan.close()

    def test_cancel_recv(self):
        chan, rchan = self.connect_pair()

        def recv():
            try:
                chan.recv(1)
            except Exception as exc:
                self.verify_exception(exc)

        def cancel():
            time.sleep(0.01)
            chan.cancel()

        par(recv, cancel)

        rchan.close()
        chan.close()

    def test_cancel_before_connect(self):
        server = self.create_server()
        chan = server.create_client()
        chan.cancel()
        try:
            chan.connect()
        except Exception as exc:
            self.verify_exception(exc)
        else:
            self.fail('Exception not raised')
        server.close()

    def test_cancel_before_close(self):
        chan, rchan = self.connect_pair()

        chan.cancel()
        try:
            chan.close()
        except Exception as exc:
            self.verify_exception(exc)

        rchan.close()

    def test_cancel_after_close(self):
        chan, rchan = self.connect_pair()

        rchan.close()
        chan.close()
        chan.cancel()

    def test_recv_timeout(self):
        chan, rchan = self.connect_pair()
        chan.set_timeout(0.01)
        try:
            chan.recv(1)
        except Exception as exc:
            self.verify_exception(exc)
        else:
            self.fail('Exception not raised')
        rchan.close()
        try:
            chan.close()
        except Exception as exc:
            self.verify_exception(exc)

    def test_send_timeout(self):
        chan, rchan = self.connect_pair()
        chan.set_timeout(0.01)
        try:
            while True:
                chan.send(b'x' * 1024 * 64)
        except Exception as exc:
            self.verify_exception(exc)
        rchan.close()
        try:
            chan.close()
        except Exception as exc:
            self.verify_exception(exc)


def snoop_socket(chan):
    for obj in chan.__dict__.values():
        if isinstance(obj, socket.SocketType):
            return obj
    raise Exception('Socket not found in %r' % (chan, ))


class TCPTest(ChannelTest):
    def create_server(self):
        return BasicSocketServer(family=socket.AF_INET, address=('localhost', 0))

    def test_nodelay(self):
        chan, rchan = self.connect_pair()

        self.assertEqual(snoop_socket(chan).getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY), 1)

        rchan.close()
        chan.close()


class UnixTest(ChannelTest):
    def create_server(self):
        return BasicSocketServer(family=socket.AF_UNIX, address=mktemp())


del ChannelTest
