import unittest
import unittest.mock
import socket
import time
import os.path

from across.channels import SocketChannel, Channel

from .utils import par, mktemp, localhost, localhost_ipv6, windows, skip_if_no_unix_sockets


def get_open_fds():
    if not windows and os.path.isdir('/proc/self/fd'):
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

    def get_address(self):
        return self.__sock.getsockname()

    def create_client(self):
        return SocketChannel(family=self.__sock.family, address=self.get_address())

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
        self.assertIsInstance(exc, OSError)

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
    raise Exception('Socket not found in {!r}'.format(chan))


class TCPTest(ChannelTest):
    def create_server(self):
        return BasicSocketServer(family=socket.AF_INET, address=(localhost, 0))

    def test_nodelay(self):
        chan, rchan = self.connect_pair()

        self.assertEqual(snoop_socket(chan).getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY), 1)

        rchan.close()
        chan.close()


class TCPTestIPv6(ChannelTest):
    def create_server(self):
        return BasicSocketServer(family=socket.AF_INET6, address=(localhost_ipv6, 0, 0, 0))

    def test_nodelay(self):
        chan, rchan = self.connect_pair()

        self.assertEqual(snoop_socket(chan).getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY), 1)

        rchan.close()
        chan.close()


class ResolverTest(unittest.TestCase):
    def test_unconnectable_address(self):
        server1 = BasicSocketServer(family=socket.AF_INET, address=(localhost, 0))
        server2 = BasicSocketServer(family=socket.AF_INET, address=(localhost, 0))
        server2.listen()
        server3 = BasicSocketServer(family=socket.AF_INET, address=(localhost, 0))
        server3.listen()

        with unittest.mock.patch('across.channels._getaddrinfo') as mock:
            fake_address = ('fake', 777)
            mock.return_value = [
                (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', server.get_address())
                for server in [server1, server2, server3]
            ]
            chan = SocketChannel(family=socket.AF_INET, address=fake_address, resolve=True)
            chan.connect()
            chan.send(b'a')
            rchan = server2.accept()
            self.assertEqual(rchan.recv(1), b'a')
            chan.close()
            rchan.close()

        server1.close()
        server2.close()
        server3.close()


@skip_if_no_unix_sockets
class UnixTest(ChannelTest):
    def create_server(self):
        return BasicSocketServer(family=socket.AF_UNIX, address=mktemp())


del ChannelTest
