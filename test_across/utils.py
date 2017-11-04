import across
import threading
import errno


class _MemoryPipe:
    def __init__(self):
        self.__lock = threading.Lock()
        self.__send_condition = threading.Condition(self.__lock)
        self.__recv_condition = threading.Condition(self.__lock)
        self.__send_data, self.__send_size = None, None
        self.__recv_data, self.__recv_size = None, None
        self.__cancelled = False

    def send(self, data):
        with self.__lock:
            if self.__send_data is not None or self.__send_size is not None:
                raise AssertionError('send already in progress')
            if self.__cancelled:
                raise IOError(errno.EPIPE, 'Pipe shut down')
            if self.__recv_size is not None:
                self.__recv_data, self.__recv_size = data[:self.__recv_size], None
                self.__recv_condition.notify()
                return len(self.__recv_data)
            self.__send_data = data
            while self.__send_size is None and not self.__cancelled:
                self.__send_condition.wait()
            if self.__send_size is None:
                raise IOError(errno.EPIPE, 'Pipe shut down')
            size, self.__send_size = self.__send_size, None
            return size

    def recv(self, size):
        with self.__lock:
            if self.__recv_data is not None or self.__recv_size is not None:
                raise AssertionError('recv already in progress')
            if self.__cancelled:
                return b''
            if self.__send_data is not None:
                data = self.__send_data[:size]
                self.__send_data, self.__send_size = None, len(data)
                self.__send_condition.notify()
                return data
            self.__recv_size = size
            while self.__recv_data is None and not self.__cancelled:
                self.__recv_condition.wait()
            if self.__recv_data is None:
                return b''
            data, self.__recv_data = self.__recv_data, None
            return data

    def cancel(self):
        with self.__lock:
            self.__cancelled = True
            self.__send_data, self.__recv_size = None, None
            self.__send_condition.notify()
            self.__recv_condition.notify()


class _MemoryPipeChannel(across.Channel):
    def __init__(self, stdin, stdout):
        self.__stdin = stdin
        self.__stdout = stdout

    def recv(self, size):
        return self.__stdin.recv(size)

    def send(self, data):
        return self.__stdout.send(data)

    def cancel(self):
        self.__stdin.cancel()
        self.__stdout.cancel()

    def close(self):
        pass


class MemoryChannel(_MemoryPipeChannel):
    def __init__(self):
        pipe1, pipe2 = _MemoryPipe(), _MemoryPipe()
        super().__init__(pipe1, pipe2)
        self.__conn = across.Connection(_MemoryPipeChannel(pipe2, pipe1))

    def close(self):
        self.__conn.close()


def make_connection():
    return across.Connection(MemoryChannel())


_box_counter = 0
_box_counter_lock = threading.Lock()
_box_values = {}


class Box(object):
    def __init__(self, value=None):
        self.value = value

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)

    def __getstate__(self):
        global _box_counter
        with _box_counter_lock:
            key = _box_counter
            _box_counter += 1
        _box_values[key] = self.value
        return key

    def __setstate__(self, key):
        self.value = _box_values.pop(key)
