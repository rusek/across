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
            args = [sys.executable, '-m', __name__]
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

    def submit(self, func, *args):
        with self.__lock:
            # TODO should be a protocol error
            assert self.__task is None
            self.__task = func, args
            self.__condition.notify()

    def cancel(self):
        with self.__lock:
            self.__cancelled = True
            self.__condition.notify()

    def __get_task(self):
        with self.__lock:
            while self.__task is None and not self.__cancelled:
                self.__condition.wait()
            if self.__cancelled:
                return None
            task, self.__task = self.__task, None
            return task

    def run(self, main=False):
        for func, args in iter(self.__get_task, None):
            is_result, value = func(*args)
            if is_result is True:
                return value
            elif is_result is False:
                raise value
        if not main:
            raise ConnectionLost


class _ActorThread(threading.Thread):
    def __init__(self, actor, tls):
        super().__init__()
        self.__actor = actor
        self.__tls = tls

    def run(self):
        self.__tls.actor = self.__actor
        self.__actor.run(main=True)


class ConnectionLost(Exception):
    pass


class ProtocolError(Exception):
    pass


_APPLY = 1
_RESULT = 2
_ERROR = 3


class _ConnScope(object):
    def __init__(self, conn):
        self.__conn = conn

    def __enter__(self):
        _conn_tls.conn, self.__conn = self.__conn,  _conn_tls.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        _conn_tls.conn = self.__conn


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

        self.__objs = {}
        self.__obj_counter = 0

    def _get_obj(self, id):
        return self.__objs[id]

    def _put_obj(self, obj):
        with self.__lock:
            id = self.__obj_counter
            self.__obj_counter += 1
        self.__objs[id] = obj
        return id

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
                # TODO should probably set error if not set
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

        with _ConnScope(self):
            payload = pickle.dumps((func, args, kwargs))

        with self.__lock:
            if self.__cancelled:
                raise ConnectionLost
            actor = self.__get_current_actor_locked()
            prefix = struct.pack('>IBQ', len(payload) + 9, _APPLY, actor.id ^ 1)
            self.__send_queue.append(prefix + payload)
            self.__send_condition.notify()
        return actor.run()

    def call(*args, **kwargs):
        try:
            self, func = args[:2]
        except ValueError:
            if not args:
                msg = "call() missing 2 required positional arguments: 'self' and 'func'"
            else:
                msg = "call() missing 1 required positional argument: 'func'"
            raise TypeError(msg) from None
        return self.apply(func, args[2:], kwargs)

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
            # TODO set error
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

    def __receiver_loop(self):
        try:
            while True:
                header = self.__recvall(4)
                if not header:
                    break
                if len(header) != 4:
                    raise ProtocolError('Incomplete message size')
                size, = struct.unpack('>I', header)
                if size == 0:
                    raise ProtocolError('Empty message')
                msg = self.__recvall(size)
                if len(msg) != size:
                    raise ProtocolError('Incomplete message')
                with self.__lock:
                    if self.__cancelled:
                        break
                    msg_type = msg[0]
                    handler = self.__handlers_locked.get(msg_type)
                    if handler is None:
                        raise ProtocolError('Invalid message type: %d' % (msg_type, ))
                    handler(self, msg)
        except Exception as error:
            self.__cancel(error)
        except:
            # TODO set error
            self.__cancel()
            raise
        else:
            self.__cancel()

    def __handle_apply_locked(self, msg):
        if len(msg) < 9:
            raise ProtocolError('Incomplete apply message')
        actor_id, = struct.unpack_from('>Q', msg, 1)
        actor = self.__actors.get(actor_id)
        if actor is None:
            if not actor_id & 1:
                raise ProtocolError('Actor not found: %d' % (actor_id, ))
            actor = self.__actors[actor_id] = _Actor(actor_id, self)
            actor_thread = _ActorThread(actor, self.__tls)
            self.__actor_threads.append(actor_thread)
            actor_thread.start()
        actor.submit(self.__process_apply, msg, actor_id)

    def __process_apply(self, msg, actor_id):
        # TODO handle pickle errors, unhandled exceptions should cancel connection

        with _ConnScope(self):
            func, args, kwargs = pickle.loads(msg[9:])
            try:
                result = func(*args, **kwargs)
            except Exception as error:
                payload = pickle.dumps(error)
                msg = struct.pack('>IBQ', len(payload) + 9, _ERROR, actor_id ^ 1) + payload
            else:
                payload = pickle.dumps(result)
                msg = struct.pack('>IBQ', len(payload) + 9, _RESULT, actor_id ^ 1) + payload

        with self.__lock:
            if not self.__cancelled:
                self.__send_queue.append(msg)
                self.__send_condition.notify()
        return None, None

    def __handle_result_locked(self, msg):
        if len(msg) < 9:
            raise ProtocolError('Incomplete result message')
        actor_id, = struct.unpack_from('>Q', msg, 1)
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_result, msg)

    def __process_result(self, msg):
        with _ConnScope(self):
            return True, pickle.loads(msg[9:])

    def __handle_error_locked(self, msg):
        if len(msg) < 9:
            raise ProtocolError('Incomplete error message')
        actor_id, = struct.unpack_from('>Q', msg, 1)
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_error, msg)

    def __process_error(self, msg):
        with _ConnScope(self):
            return False, pickle.loads(msg[9:])

    __handlers_locked = {
        _APPLY: __handle_apply_locked,
        _RESULT: __handle_result_locked,
        _ERROR: __handle_error_locked,
    }

    def wait(self):
        with self.__lock:
            while not self.__cancelled:
                self.__cancel_condition.wait()

    def create(*args, **kwargs):
        try:
            self, func = args[:2]
        except ValueError:
            if not args:
                msg = "create() missing 2 required positional arguments: 'self' and 'func'"
            else:
                msg = "create() missing 1 required positional argument: 'func'"
            raise TypeError(msg) from None
        return self.apply(_apply_local, (func, args[2:], kwargs))

    def replicate(self, value):
        return self.apply(Local, (value, ))


class Proxy(object):
    def __init__(self, id):
        self._Proxy__conn = get_connection()
        self.__id = id

    def __deepcopy__(self, memo):
        return self._Proxy__conn.apply(_get_obj, (self.__id, ))

    def __reduce__(self):
        conn = get_connection()
        if conn is not self._Proxy__conn:
            raise ValueError
        return _get_obj, (self.__id, )


_proxy_types = {}
_safe_magic = frozenset([
    '__contains__', '__delitem__', '__getitem__', '__len__', '__setitem__', '__call__'])


def _make_auto_proxy_type(cls):
    scope = {'Proxy': Proxy, '_apply_method': _apply_method}
    source = ['class _AutoProxy(Proxy):\n']

    for attr in dir(cls):
        if not callable(getattr(cls, attr)) or (attr.startswith('_') and attr not in _safe_magic):
            continue
        source.append(
            '    def %(name)s(self, *args, **kwargs):\n'
            '        return self._Proxy__conn.apply(_apply_method, (self, %(name)r, args, kwargs))\n'
            % dict(name=attr)
        )

    if len(source) == 1:
        source.append('    pass\n')

    exec(''.join(source), scope)
    return scope['_AutoProxy']


class Local(object):
    def __init__(self, value):
        assert not isinstance(value, (Proxy, Local))
        self.value = value

    def __reduce__(self):
        return _make_proxy, (type(self.value), get_connection()._put_obj(self.value))


def _apply_method(obj, name, args, kwargs):
    return getattr(obj, name)(*args, **kwargs)


def _get_obj(id):
    return get_connection()._get_obj(id)


def _apply_local(func, args, kwargs):
    return Local(func(*args, **kwargs))


def _make_proxy(cls, id):
    proxy_type = _proxy_types.get(cls)
    if proxy_type is None:
        proxy_type = _proxy_types[cls] = _make_auto_proxy_type(cls)
    return proxy_type(id)


def get_connection():
    conn = _conn_tls.conn
    if conn is None:
        raise RuntimeError
    return conn
