import threading
import subprocess
import select
import sys
import os
import fcntl


class Channel:
    def recv(self, size):
        raise NotImplementedError

    def send(self, data):
        raise NotImplementedError

    def cancel(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class PipeChannel(Channel):
    def __init__(self, stdin, stdout):
        stdout.flush()
        fcntl.fcntl(stdin, fcntl.F_SETFL, fcntl.fcntl(stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
        self.__stdin = stdin
        self.__stdout = stdout
        self.__stdout_fd = stdout.fileno()
        self.__shutdown_rfd, self.__shutdown_wfd = os.pipe()
        self.__close_lock = threading.Lock()

    def recv(self, size):
        data = self.__stdin.read(size)
        if data is not None:
            return data

        readable_fds, _, _ = select.select([self.__stdin, self.__shutdown_rfd], [], [])
        if self.__shutdown_rfd in readable_fds:
            raise ValueError('recv on cancelled channel')

        data = self.__stdin.read(size)
        assert data is not None
        return data

    def send(self, data):
        readable_fds, _, _ = select.select([self.__shutdown_rfd], [self.__stdout_fd], [])
        if readable_fds:
            raise ValueError('send on cancelled channel')
        return os.write(self.__stdout_fd, data)

    def cancel(self):
        with self.__close_lock:
            if self.__shutdown_wfd is not None:
                os.write(self.__shutdown_wfd, b'\0')

    def close(self):
        with self.__close_lock:
            os.close(self.__shutdown_rfd)
            os.close(self.__shutdown_wfd)
            self.__shutdown_rfd, self.__shutdown_wfd = None, None


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
