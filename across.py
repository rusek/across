import threading
import collections
import struct
import pickle
import subprocess
import select
import sys
import os
import errno


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
        self.__stdin = stdin
        self.__stdin_fd = stdin.fileno()
        self.__stdout = stdout
        self.__stdout_fd = stdout.fileno()
        self.__shutdown_rfd, self.__shutdown_wfd = os.pipe()

    def recv(self, size):
        readable_fds, _, _ = select.select([self.__stdin_fd, self.__shutdown_rfd], [], [])
        if self.__shutdown_rfd in readable_fds:
            return b''
        return os.read(self.__stdin_fd, size)

    def send(self, data):
        readable_fds, _, _ = select.select([self.__shutdown_rfd], [self.__stdout_fd], [])
        if readable_fds:
            raise IOError(errno.EPIPE, 'Connection shut down')
        return os.write(self.__stdout_fd, data)

    def cancel(self):
        os.write(self.__shutdown_wfd, b'\0')

    def close(self):
        os.close(self.__shutdown_rfd)
        os.close(self.__shutdown_wfd)


class ProcessChannel(PipeChannel):
    def __init__(self, args=None):
        if args is None:
            args = [sys.executable, '-c', boot]
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


boot = """
from across import _main
_main()
"""


class _ConnTls(threading.local):
    def __init__(self):
        self.conn = None

_conn_tls = _ConnTls()


class _Actor:
    def __init__(self, id, conn):
        self.id = id
        self.__conn = conn
        self.__lock = threading.Lock()
        self.__condition = threading.Condition(self.__lock)
        self.__task = None
        self.__cancelled = False

    def append_apply(self, payload):
        with self.__lock:
            assert self.__task is None
            self.__task = False, payload
            self.__condition.notify()

    def append_result(self, payload):
        with self.__lock:
            assert self.__task is None
            self.__task = True, payload
            self.__condition.notify()

    def cancel(self):
        with self.__lock:
            self.__cancelled = True
            self.__condition.notify()

    def run(self, main=False):
        while True:
            with self.__lock:
                while self.__task is None and not self.__cancelled:
                    self.__condition.wait()
                if self.__cancelled:
                    if main:
                        return None
                    else:
                        raise ConnectionBroken
                is_result, payload = self.__task
                self.__task = None
            old_conn = _conn_tls.conn
            _conn_tls.conn = self.__conn
            try:
                obj = pickle.loads(payload)
                if is_result:
                    return obj
                else:
                    func, args, kwargs = obj
                    result = func(*args, **kwargs)
                    self.__conn._actor_result_hook(self.id, result)
            finally:
                _conn_tls.conn = old_conn


class _ActorThread(threading.Thread):
    def __init__(self, actor, tls):
        super().__init__()
        self.__actor = actor
        self.__tls = tls

    def run(self):
        self.__tls.actor = self.__actor
        self.__actor.run(main=True)


class ConnectionBroken(Exception):
    pass


class Connection:
    def __init__(self, channel):
        self.__channel = channel
        self.__lock = threading.Lock()
        self.__send_queue = collections.deque()
        self.__send_condition = threading.Condition(self.__lock)
        self.__sender_thread = threading.Thread(target=self.__sender_loop)
        self.__receiver_thread = threading.Thread(target=self.__receiver_loop)
        self.__cancelled = False
        self.__cancel_condition = threading.Condition(self.__lock)
        self.__cancel_error = None

        self.__actors = {}
        self.__next_actor_id = 0
        self.__tls = threading.local()
        self.__actor_threads = []

        self.__sender_thread.start()
        self.__receiver_thread.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.__cancel()
        self.__sender_thread.join()
        self.__receiver_thread.join()
        for actor_thread in self.__actor_threads:
            actor_thread.join()
        # TODO handle errors
        self.__channel.close()
        if self.__cancel_error is not None:
            try:
                raise self.__cancel_error
            finally:
                self.__cancel_error = None

    def __cancel(self, error=None):
        with self.__lock:
            if self.__cancelled:
                return
            self.__cancelled = True
            self.__cancel_error = error
            self.__send_condition.notify()
            self.__cancel_condition.notify_all()
            for actor in self.__actors.values():
                actor.cancel()
            self.__channel.cancel()

    def __get_current_actor_locked(self):
        try:
            return self.__tls.actor
        except AttributeError:
            actor_id = self.__next_actor_id
            self.__next_actor_id += 2
            actor = self.__actors[actor_id] = self.__tls.actor = _Actor(actor_id, self)
            return actor

    def apply(self, func, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        old_conn = _conn_tls.conn
        _conn_tls.conn = self
        try:
            payload = pickle.dumps((func, args, kwargs))
        finally:
            _conn_tls.conn = old_conn

        with self.__lock:
            if self.__cancelled:
                raise ConnectionBroken
            actor = self.__get_current_actor_locked()
            header = struct.pack('>BII', 0, actor.id ^ 1, len(payload))
            self.__send_queue.append(header + payload)
            self.__send_condition.notify()
        return actor.run()

    def call(*args, **kwargs):
        self, func, args = (lambda s, f, *a: (s, f, a))(*args)
        return self.apply(func, args, kwargs)

    def __sender_loop(self):
        try:
            while True:
                with self.__lock:
                    while not self.__send_queue and not self.__cancelled:
                        self.__send_condition.wait()
                    if self.__cancelled:
                        break
                    data = self.__send_queue.popleft()
                self.__sendall(data)
        except Exception as error:
            self.__cancel(error)
        except:
            self.__cancel()
            raise
        else:
            self.__cancel()

    def __sendall(self, data):
        while data:
            data = data[self.__channel.send(data):]

    def __recvall(self, size):
        data = b''
        while size:
            chunk = self.__channel.recv(size)
            if not chunk:
                return data
            data += chunk
            size -= len(chunk)
        return data

    def _actor_result_hook(self, actor_id, result):
        # FIXME handle exceptions
        payload = pickle.dumps(result)
        header = struct.pack('>BII', 1, actor_id ^ 1, len(payload))
        with self.__lock:
            if not self.__cancelled:
                self.__send_queue.append(header + payload)
                self.__send_condition.notify()

    def __receiver_loop(self):
        try:
            while True:
                header = self.__recvall(9)
                if not header:
                    break
                assert len(header) == 9
                msg_type, actor_id, payload_size = struct.unpack('>BII', header)
                payload = self.__recvall(payload_size)
                assert len(payload) == payload_size
                with self.__lock:
                    if self.__cancelled:
                        break
                    if msg_type == 0:
                        actor = self.__actors.get(actor_id)
                        if actor is None:
                            assert actor_id & 1
                            actor = self.__actors[actor_id] = _Actor(actor_id, self)
                            actor_thread = _ActorThread(actor, self.__tls)
                            self.__actor_threads.append(actor_thread)
                            actor_thread.start()
                        actor.append_apply(payload)
                    else:
                        actor = self.__actors[actor_id]
                        actor.append_result(payload)
        except Exception as error:
            self.__cancel(error)
        except:
            self.__cancel()
            raise
        else:
            self.__cancel()

    def wait(self):
        with self.__lock:
            while not self.__cancelled:
                self.__cancel_condition.wait()


def get_connection():
    conn = _conn_tls.conn
    if conn is None:
        raise RuntimeError
    return conn


def _main():
    channel = PipeChannel(sys.stdin.buffer, sys.stdout.buffer)
    with Connection(channel) as conn:
        conn.wait()
