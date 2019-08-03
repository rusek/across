import threading
import subprocess
import select
import os
import sys
import socket
import errno
import signal

from ._utils import logger as _logger


_windows = (sys.platform == 'win32')


class Channel:
    def set_timeout(self, timeout):
        pass

    def connect(self):
        pass

    def recv_into(self, buffer):
        raise NotImplementedError

    def send(self, buffer):
        raise NotImplementedError

    def cancel(self):
        pass

    def close(self):
        pass


def _make_timeout_error():
    return OSError(errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))


def _make_cancelled_error():
    return OSError(errno.ECANCELED, os.strerror(errno.ECANCELED))


if _windows:
    class _Poller:
        def __init__(self):
            self.__pipe = None
            self.__cancel_lock = threading.Lock()
            self.__cancelled = False
            self.__send_fd = None
            self.__recv_fd = None
            self.__timeout = None

        def __create_pipe(self):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sock.listen(1)
            in_pipe = socket.socket()
            in_pipe.connect(sock.getsockname())
            out_pipe = sock.accept()[0]
            sock.close()
            self.__pipe = in_pipe, out_pipe

        def set_timeout(self, timeout):
            self.__timeout = timeout

        def assign(self, send_fd, recv_fd):
            with self.__cancel_lock:
                if self.__cancelled:
                    raise _make_cancelled_error()
                if not self.__pipe:
                    self.__create_pipe()
                self.__send_fd = send_fd
                self.__recv_fd = recv_fd

        def wait_recv(self):
            rlist, _, _ = select.select([self.__pipe[0], self.__recv_fd], [], [], self.__timeout)
            if self.__cancelled:
                raise _make_cancelled_error()
            if not rlist:
                raise _make_timeout_error()

        def wait_send(self):
            _, wlist, elist = select.select([self.__pipe[0]], [self.__send_fd], [self.__send_fd], self.__timeout)
            if self.__cancelled:
                raise _make_cancelled_error()
            if not wlist and not elist:
                raise _make_timeout_error()

        def cancel(self):
            with self.__cancel_lock:
                self.__cancelled = True
                if self.__pipe:
                    self.__pipe[1].send(b'\0')

        def is_cancelled(self):
            return self.__cancelled

        def close(self):
            with self.__cancel_lock:
                if self.__pipe:
                    self.__pipe[0].close()
                    self.__pipe[1].close()
                    self.__pipe = None
else:
    class _Poller:
        def __init__(self):
            self.__pipe = None
            self.__cancel_lock = threading.Lock()
            self.__cancelled = False
            self.__send_poll = None
            self.__recv_poll = None
            self.__timeout_ms = None

        def set_timeout(self, timeout):
            self.__timeout_ms = max(1, int(round(timeout * 1000)))

        def assign(self, send_fd, recv_fd):
            with self.__cancel_lock:
                if self.__cancelled:
                    raise _make_cancelled_error()
                if not self.__pipe:
                    self.__pipe = os.pipe()
                self.__send_poll = select.poll()
                self.__send_poll.register(self.__pipe[0], select.POLLIN)
                self.__send_poll.register(send_fd, select.POLLOUT)
                self.__recv_poll = select.poll()
                self.__recv_poll.register(self.__pipe[0], select.POLLIN)
                self.__recv_poll.register(recv_fd, select.POLLIN)

        def wait_recv(self):
            if not self.__recv_poll.poll(self.__timeout_ms):
                raise _make_timeout_error()
            if self.__cancelled:
                raise _make_cancelled_error()

        def wait_send(self):
            if not self.__send_poll.poll(self.__timeout_ms):
                raise _make_timeout_error()
            if self.__cancelled:
                raise _make_cancelled_error()

        def cancel(self):
            with self.__cancel_lock:
                self.__cancelled = True
                if self.__pipe:
                    os.write(self.__pipe[1], b'\0')

        def is_cancelled(self):
            return self.__cancelled

        def close(self):
            with self.__cancel_lock:
                if self.__pipe:
                    os.close(self.__pipe[0])
                    os.close(self.__pipe[1])
                    self.__pipe = None

        def __del__(self):
            self.close()


if _windows:
    # No support for timeouts and cancellation on Windows.
    class PipeChannel(Channel):
        def __init__(self, in_pipe, out_pipe, close, logger=_logger):
            self.__in_pipe = in_pipe
            self.__out_pipe = out_pipe
            self.__close = close

        def recv_into(self, buffer):
            return self.__in_pipe.readinto(buffer)

        def send(self, buffer):
            self.__out_pipe.write(buffer)
            self.__out_pipe.flush()
            return len(buffer)

        def close(self):
            if self.__close:
                self.__in_pipe.close()
                self.__out_pipe.close()
else:
    import fcntl

    class PipeChannel(Channel):
        def __init__(self, in_pipe, out_pipe, close, logger=_logger):
            out_pipe.flush()
            fcntl.fcntl(in_pipe, fcntl.F_SETFL, fcntl.fcntl(in_pipe, fcntl.F_GETFL) | os.O_NONBLOCK)
            self.__in_pipe = in_pipe
            self.__out_pipe = out_pipe
            self.__out_fd = out_pipe.fileno()
            self.__poller = _Poller()
            self.__poller.assign(self.__out_fd, self.__in_pipe.fileno())
            self.__close = close

        def set_timeout(self, timeout):
            self.__poller.set_timeout(timeout)

        def recv_into(self, buffer):
            nbytes = self.__in_pipe.readinto(buffer)
            if nbytes is not None:
                return nbytes
            self.__poller.wait_recv()
            nbytes = self.__in_pipe.readinto(buffer)
            assert nbytes is not None
            return nbytes

        def send(self, buffer):
            self.__poller.wait_send()
            return os.write(self.__out_fd, buffer)

        def cancel(self):
            self.__poller.cancel()

        def close(self):
            self.__poller.close()
            if self.__close:
                self.__in_pipe.close()
                self.__out_pipe.close()

        def __repr__(self):
            return '<{} {!r} {!r}>'.format(self.__class__.__name__, self.__in_pipe, self.__out_pipe)


class ProcessChannel(PipeChannel):
    def __init__(self, args=None, proc=None, logger=_logger, **popen_extras):
        if proc is None:
            if args is None:
                raise ValueError('Arguments must be provided if process is omitted')
            proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, **popen_extras)
            logger.debug('Started child process %r, pid=%r', args, proc.pid)
        else:
            if proc.stdin is None:
                raise ValueError('Process standart input is not a pipe')
            if proc.stdout is None:
                raise ValueError('Process standart output is not a pipe')
        super().__init__(proc.stdout, proc.stdin, close=True, logger=logger)
        self.__process = proc
        self.__args = args
        self.__logger = logger
        self.__timeout = None

    def set_timeout(self, timeout):
        self.__timeout = timeout
        super().set_timeout(timeout)

    def cancel(self):
        self.__kill_process()
        super().cancel()

    def __kill_process(self):
        if self.__process.poll() is not None:
            return
        self.__logger.debug('Killing process %r', self.__process.pid)
        try:
            self.__process.kill()
        except OSError:  # process already terminated
            pass

    def close(self):
        super().close()
        self.__logger.debug('Joining process %r', self.__process.pid)
        try:
            retcode = self.__process.wait(self.__timeout)
        except subprocess.TimeoutExpired:
            self.__kill_process()
            retcode = self.__process.wait()
        self.__logger.debug('Process joined, retcode=%r', retcode)
        if retcode:
            raise subprocess.CalledProcessError(retcode, self.__args)

    def __repr__(self):
        return '<{} {!r}>'.format(self.__class__.__name__, self.__process.args)


_getaddrinfo = socket.getaddrinfo  # patched by tests


class SocketChannel(Channel):
    def __init__(self, family=None, address=None, sock=None, resolve=False, logger=_logger):
        if sock is None:
            if address is None:
                raise ValueError('Address must be provided if socket is omitted')
            if family is None:
                raise ValueError('Family must be provided if socket is omitted')
        else:
            if family is not None:
                raise ValueError('Family must be omitted if socket is provided')
            family = sock.family
        if address is None:
            if resolve:
                raise ValueError('Resolver may be used only when address is provided')
        self.__family = family
        self.__address = address
        self.__resolve = resolve
        self.__sock = sock
        self.__poller = _Poller()
        self.__logger = logger
        if self.__sock is not None:
            self.__prepare_socket()

    def set_timeout(self, timeout):
        self.__poller.set_timeout(timeout)

    def __prepare_socket(self):
        self.__tune_socket()
        self.__sock.setblocking(False)
        self.__poller.assign(self.__sock.fileno(), self.__sock.fileno())

    def __tune_socket(self):
        if self.__sock.family in (socket.AF_INET, socket.AF_INET6):
            self.__sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def __connect_once(self, family, address):
        if self.__sock is None:
            self.__sock = socket.socket(family, socket.SOCK_STREAM)
            self.__prepare_socket()
        try:
            self.__logger.debug('Trying to connect to %r', address)
            self.__sock.connect(address)
            return
        except BlockingIOError:
            pass
        self.__poller.wait_send()
        error = self.__sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if error != 0:
            raise OSError(error, os.strerror(error))

    def __connect_loop(self):
        if self.__resolve:
            host, port = self.__address
            addrinfo = _getaddrinfo(host, port, self.__family, socket.SOCK_STREAM)
        else:
            addrinfo = [(self.__family, None, None, None, self.__address)]

        last_error = None
        for family, _, _, _, address in addrinfo:
            had_socket = self.__sock is not None
            try:
                self.__connect_once(family, address)
                return
            except OSError as error:
                if self.__poller.is_cancelled():
                    raise
                if not had_socket and self.__sock is not None:
                    self.__sock.close()
                    self.__sock = None

                self.__logger.debug('connect throws %r', error)
                last_error = error

        if last_error is None:
            # C getaddrinfo() returns one or more items, so this should hopefully never happen
            raise RuntimeError('getaddrinfo returned empty list')
        else:
            try:
                raise last_error
            finally:
                last_error = None

    def connect(self):
        if self.__address:
            self.__connect_loop()

    def send(self, buffer):
        self.__poller.wait_send()
        return self.__sock.send(buffer)

    def recv_into(self, buffer):
        self.__poller.wait_recv()
        return self.__sock.recv_into(buffer)

    def cancel(self):
        self.__poller.cancel()

    def close(self):
        self.__poller.close()
        if self.__sock is not None:
            self.__sock.close()

    def __repr__(self):
        return '<{} {!r}>'.format(self.__class__.__name__, self.__sock or self.__address)
