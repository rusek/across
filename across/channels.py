import threading
import subprocess
import select
import sys
import os
import fcntl
import socket
import errno


class Channel:
    def connect(self):
        pass

    def recv(self, size):
        raise NotImplementedError

    def send(self, data):
        raise NotImplementedError

    def cancel(self):
        pass

    def close(self):
        pass


class _Poller:
    def __init__(self):
        self.__pipe = None
        self.__cancel_lock = threading.Lock()
        self.__cancelled = False
        self.__send_poll = select.poll()
        self.__recv_poll = select.poll()

    def setup(self, send_fd, recv_fd):
        with self.__cancel_lock:
            if self.__cancelled:
                raise ValueError('Channel is cancelled')
            self.__pipe = os.pipe()
            self.__send_poll.register(self.__pipe[0], select.POLLIN)
            self.__send_poll.register(send_fd, select.POLLOUT)
            self.__recv_poll.register(self.__pipe[0], select.POLLIN)
            self.__recv_poll.register(recv_fd, select.POLLIN)

    def wait_recv(self):
        self.__recv_poll.poll()
        if self.__cancelled:
            raise ValueError('Channel is cancelled')

    def wait_send(self):
        self.__send_poll.poll()
        if self.__cancelled:
            raise ValueError('Channel is cancelled')

    def cancel(self):
        with self.__cancel_lock:
            self.__cancelled = True
            if self.__pipe:
                os.write(self.__pipe[1], b'\0')

    def close(self):
        with self.__cancel_lock:
            if self.__pipe:
                os.close(self.__pipe[0])
                os.close(self.__pipe[1])
                self.__pipe = None


class PipeChannel(Channel):
    def __init__(self, stdin, stdout):
        stdout.flush()
        fcntl.fcntl(stdin, fcntl.F_SETFL, fcntl.fcntl(stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
        self.__stdin = stdin
        self.__stdout = stdout
        self.__stdout_fd = stdout.fileno()
        self.__poller = _Poller()
        self.__poller.setup(self.__stdout_fd, self.__stdin.fileno())

    def recv(self, size):
        data = self.__stdin.read(size)
        if data is not None:
            return data
        self.__poller.wait_recv()
        data = self.__stdin.read(size)
        assert data is not None
        return data

    def send(self, data):
        self.__poller.wait_send()
        return os.write(self.__stdout_fd, data)

    def cancel(self):
        self.__poller.cancel()

    def close(self):
        self.__poller.close()


class ProcessChannel(PipeChannel):
    def __init__(self, args=None):
        if args is None:
            args = [sys.executable, '-m', 'across']
        process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        super().__init__(process.stdout, process.stdin)
        self.__process = process
        self.__args = args

    def close(self):
        super().close()
        self.__process.stdin.close()
        self.__process.stdout.close()
        retcode = self.__process.wait()
        if retcode:
            raise subprocess.CalledProcessError(retcode, self.__args)


class SocketChannel(Channel):
    def __init__(self, family=None, address=None, sock=None):
        if sock is None:
            if address is None:
                raise ValueError('Address must be provided if socket is omitted')
            if family is None:
                raise ValueError('Family must be provided if socket is omitted')
        else:
            if family is not None:
                raise ValueError('Family must be omitted if socket is provided')
            family = sock.family
        self.__family = family
        self.__address = address
        self.__sock = sock
        self.__poller = _Poller()

    def __tune_socket(self):
        if self.__family in (socket.AF_INET, socket.AF_INET6):
            self.__sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def connect(self):
        try:
            if self.__sock is None:
                self.__sock = socket.socket(self.__family)
            self.__tune_socket()
            self.__sock.setblocking(False)
            self.__poller.setup(self.__sock.fileno(), self.__sock.fileno())
            if self.__address:
                try:
                    self.__sock.connect(self.__address)
                except socket.error as error:
                    if error.errno != errno.EINPROGRESS:
                        raise
                else:
                    return
                self.__poller.wait_send()
                error = self.__sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if error != 0:
                    raise socket.error(error)
        except BaseException:
            self.__poller.close()
            if self.__sock:
                self.__sock.close()
            raise

    def send(self, data):
        self.__poller.wait_send()
        return self.__sock.send(data)

    def recv(self, size):
        self.__poller.wait_recv()
        return self.__sock.recv(size)

    def cancel(self):
        self.__poller.cancel()

    def close(self):
        self.__poller.close()
        self.__sock.close()
