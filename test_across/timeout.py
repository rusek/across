import unittest.mock
import unittest
import queue
import threading
import collections

import across
import across.channels
import errno
from test_across.utils import MemoryChannel


class EOFChannel(across.channels.Channel):
    def send(self, data):
        raise IOError(errno.EPIPE)

    def recv(self, size):
        return b''


class TimeoutableQueue:
    last = None

    def __init__(self):
        self.__lock = threading.Lock()
        self.__condition = threading.Condition(self.__lock)
        self.__queue = collections.deque()
        self.__simulated_timeouts = 0
        self.__last_get_timeout = None
        TimeoutableQueue.last = self

    def simulate_timeout(self):
        with self.__lock:
            self.__simulated_timeouts += 1
            self.__condition.notify()

    def get(self, timeout=None):
        with self.__lock:
            self.__last_get_timeout = timeout
            while not self.__queue:
                if self.__simulated_timeouts and timeout is not None:
                    self.__simulated_timeouts -= 1
                    raise queue.Empty
                self.__condition.wait()
            return self.__queue.popleft()

    def get_last_get_timeout(self):
        return self.__last_get_timeout

    def put(self, item):
        with self.__lock:
            self.__queue.append(item)
            self.__condition.notify()


class ByteCountingMemoryChannel(MemoryChannel):
    def __init__(self):
        super().__init__()
        self.__lock = threading.Lock()
        self.__condition = threading.Condition(self.__lock)
        self.__num_recv_bytes = 0

    def get_recv_bytes(self):
        return self.__num_recv_bytes

    def wait_recv_bytes_changed(self, old_value):
        with self.__lock:
            while self.__num_recv_bytes == old_value:
                self.__condition.wait()

    def recv(self, size):
        data = super().recv(size)
        with self.__lock:
            self.__num_recv_bytes += len(data)
            self.__condition.notify_all()
        return data


def nop():
    pass


class TimeoutTest(unittest.TestCase):
    @unittest.mock.patch('across._SimpleQueue', new=TimeoutableQueue)
    def test_idle_messages(self):
        timeout = 10
        TimeoutableQueue.last = None
        chan = ByteCountingMemoryChannel()
        remote_sender_queue = TimeoutableQueue.last
        with across.Connection(chan, timeout=timeout) as conn:
            conn.call(nop)  # ensure handshake is done
            for _ in range(10):
                recv_bytes = chan.get_recv_bytes()
                remote_sender_queue.simulate_timeout()
                chan.wait_recv_bytes_changed(recv_bytes)
            last_get_timeout = remote_sender_queue.get_last_get_timeout()
            self.assertIsNotNone(last_get_timeout)
            self.assertLess(last_get_timeout, timeout * 0.75)
            self.assertGreater(last_get_timeout, timeout * 0.1)

    @unittest.mock.patch('across._SimpleQueue', new=TimeoutableQueue)
    def test_no_idle_messages(self):
        timeout = None
        TimeoutableQueue.last = None
        chan = ByteCountingMemoryChannel()
        remote_sender_queue = TimeoutableQueue.last
        with across.Connection(chan, timeout=timeout) as conn:
            conn.call(nop)  # ensure handshake is done
            self.assertIsNone(remote_sender_queue.get_last_get_timeout())

    def test_valid_timeouts(self):
        for timeout in [
            10,
            10.0
        ]:
            with across.Connection(MemoryChannel(), timeout=timeout) as conn:
                conn.call(nop)

    def test_large_timeout(self):
        with across.Connection(MemoryChannel(), timeout=1.0e100) as conn:
            conn.call(nop)

    def test_invalid_timeout_value(self):
        for timeout in [
            -1,
            -1.0,
            float('nan'),
            float('-inf'),
        ]:
            with self.assertRaises(ValueError):
                across.Connection(EOFChannel(), timeout=timeout)

    def test_invalid_timeout_type(self):
        for timeout in [
            [],
            '12.0',
        ]:
            with self.assertRaises(TypeError):
                across.Connection(EOFChannel(), timeout=timeout)