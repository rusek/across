import threading
import collections
import struct
import pickle
import subprocess
import select
import sys
import os
import types
import atexit
import traceback
import fcntl


_version = (0, 1, 0)
__version__ = '%d.%d.%d' % _version


class BlockingChannel:
    def recv(self, size):
        raise NotImplementedError

    def send(self, data):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class PipeChannel(BlockingChannel):
    def __init__(self, stdin, stdout):
        stdout.flush()
        fcntl.fcntl(stdin, fcntl.F_SETFL, fcntl.fcntl(stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
        self.__stdin = stdin
        self.__stdout = stdout
        self.__stdout_fd = stdout.fileno()
        self.__shutdown_rfd, self.__shutdown_wfd = os.pipe()
        self.__recv_lock = threading.Lock()
        self.__send_lock = threading.Lock()

    def recv(self, size):
        with self.__recv_lock:
            data = self.__stdin.read(size)
            if data is not None:
                return data

            readable_fds, _, _ = select.select([self.__stdin, self.__shutdown_rfd], [], [])
            if self.__shutdown_rfd in readable_fds:
                return None

            data = self.__stdin.read(size)
            assert data is not None
            return data

    def send(self, data):
        with self.__send_lock:
            readable_fds, _, _ = select.select([self.__shutdown_rfd], [self.__stdout_fd], [])
            if readable_fds:
                return None
            return os.write(self.__stdout_fd, data)

    def close(self):
        os.write(self.__shutdown_wfd, b'\0')
        with self.__send_lock, self.__recv_lock:
            os.close(self.__shutdown_rfd)
            os.close(self.__shutdown_wfd)


class ProcessChannel(PipeChannel):
    @staticmethod
    def get_bios():
        to_skip = len(_get_greeting_frame())
        to_send = struct.pack('>IBI', 5, _GREETING_BOOTSTRAP, _greeting_magic)
        return ("import sys;i,o=sys.stdin.buffer,sys.stdout.buffer;o.write(%r);o.flush();i.read(%r);"
                "exec(i.readline())" % (to_send, to_skip))

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
        self.pickling = False


_conn_tls = _ConnTls()


class _Actor:
    def __init__(self, id):
        self.id = id
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
            ret = func(*args)
            if ret is not None:
                return ret
        if not main:
            raise DisconnectError


class _ActorThread(threading.Thread):
    def __init__(self, actor, tls):
        super().__init__(daemon=True)
        self.__actor = actor
        self.__tls = tls

    def run(self):
        self.__tls.actor = self.__actor
        self.__actor.run(main=True)


class OperationError(Exception):
    pass


class DisconnectError(OperationError):
    pass


class ProtocolError(Exception):
    pass


class CancelledError(Exception):
    pass


_GREETING = 0
_APPLY = 1
_RESULT = 2
_ERROR = 3
_OPERATION_ERROR = 4
_GREETING_BOOTSTRAP = 5

_message_types = frozenset([
    _GREETING,
    _APPLY,
    _RESULT,
    _ERROR,
    _OPERATION_ERROR,
    _GREETING_BOOTSTRAP,
])


class _ConnScope(object):
    def __init__(self, conn, pickling=False):
        self.__conn = conn
        self.__pickling = pickling

    def __enter__(self):
        _conn_tls.conn, self.__conn = self.__conn,  _conn_tls.conn
        _conn_tls.pickling, self.__pickling = self.__pickling, _conn_tls.pickling

    def __exit__(self, exc_type, exc_val, exc_tb):
        _conn_tls.conn = self.__conn
        _conn_tls.pickling = self.__pickling


_greeting_magic = 0x02393e73


def _get_greeting_frame():
    return struct.pack('>IBIBBBBBB', 11, _GREETING, _greeting_magic, *sys.version_info[:3] + _version)


_active_connections = set()


class Connection:
    def __init__(self, channel):
        self.__channel = channel
        self.__lock = threading.Lock()
        self.__send_condition = threading.Condition(self.__lock)
        self.__sender_thread = threading.Thread(target=self.__sender_loop, daemon=True)
        self.__receiver_thread = threading.Thread(target=self.__receiver_loop, daemon=True)
        self.__cancelled = False
        self.__ready_condition = threading.Condition(self.__lock)
        self.__cancel_condition = threading.Condition(self.__lock)
        self.__cancel_error = None
        if type(self) is _BootstrappedConnection:
            self.__handlers_locked = self.__ready_handlers_locked
            self.__ready = True
            self.__send_queue = collections.deque()
        else:
            self.__handlers_locked = self.__greeting_handlers_locked
            self.__ready = False
            self.__send_queue = collections.deque([_get_greeting_frame()])

        self.__actors = {}
        self.__next_actor_id = 0
        self.__tls = threading.local()
        self.__actor_threads = []

        self.__objs = {}
        self.__obj_counter = 0

        self.__sender_thread.start()
        self.__receiver_thread.start()

        _active_connections.add(self)

    def _get_obj(self, id):
        return self.__objs[id]

    def _put_obj(self, obj):
        with self.__lock:
            id = self.__obj_counter
            self.__obj_counter += 1
        self.__objs[id] = obj
        return id

    def _del_obj(self, id):
        del self.__objs[id]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.close()
        else:
            self.__cancel()
            try:
                self.close()
            except Exception:
                pass

    def close(self):
        try:
            _active_connections.remove(self)
        except KeyError:
            return  # already closed
        self.__cancel()
        self.__sender_thread.join()
        self.__receiver_thread.join()
        for actor_thread in self.__actor_threads:
            actor_thread.join()
        if self.__cancel_error is not None:
            try:
                raise self.__cancel_error
            finally:
                self.__cancel_error = None

    def cancel(self):
        self.__cancel(CancelledError())

    def __cancel(self, error=None):
        with self.__lock:
            if error is not None and self.__cancel_error is None:
                self.__cancel_error = error
            if self.__cancelled:
                return
            self.__ready = True
            self.__ready_condition.notify_all()
            self.__cancelled = True
            self.__send_condition.notify()
            self.__cancel_condition.notify_all()
            for actor in self.__actors.values():
                actor.cancel()
            try:
                self.__channel.close()
            except Exception as error:
                if self.__cancel_error is None:
                    self.__cancel_error = error

    def __get_current_actor_locked(self):
        try:
            return self.__tls.actor
        except AttributeError:
            actor_id = self.__next_actor_id
            self.__next_actor_id += 2
            actor = self.__actors[actor_id] = self.__tls.actor = _Actor(actor_id)
            return actor

    def __enqueue_send_locked(self, frame):
        self.__send_queue.append(frame)
        self.__send_condition.notify()

    def call(*args, **kwargs):
        try:
            (self, func), args = args[:2], args[2:]
        except ValueError:
            if not args:
                msg = "call() missing 2 required positional arguments: 'self' and 'func'"
            else:
                msg = "call() missing 1 required positional argument: 'func'"
            raise TypeError(msg) from None

        payload = self.__pickle((func, args, kwargs))

        with self.__lock:
            while not self.__ready:
                self.__ready_condition.wait()
            if self.__cancelled:
                raise DisconnectError
            actor = self.__get_current_actor_locked()
            prefix = struct.pack('>IBQ', len(payload) + 9, _APPLY, actor.id ^ 1)
            self.__enqueue_send_locked(prefix + payload)
        success, value = actor.run()
        if success:
            return value
        else:
            raise value

    def __pickle(self, obj):
        with _ConnScope(self, pickling=True):
            try:
                return pickle.dumps(obj)
            except Exception as error:
                raise OperationError('Pickling failed: %s' % (error, ))

    def __unpickle(self, data):
        with _ConnScope(self):
            try:
                return pickle.loads(data)
            except Exception as error:
                raise OperationError('Unpickling failed: %s' % (error, ))

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
        else:
            self.__cancel()

    def __sendall(self, data):
        while data:
            size = self.__channel.send(data)
            if size is None:
                break
            data = data[size:]

    def __recvall(self, size):
        data = b''
        while size:
            chunk = self.__channel.recv(size)
            if not chunk:
                if chunk is None:
                    return None
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
                if msg is None:
                    break
                if len(msg) != size:
                    raise ProtocolError('Incomplete message')
                with self.__lock:
                    if self.__cancelled:
                        break
                    msg_type = msg[0]
                    handler = self.__handlers_locked.get(msg_type)
                    if handler is None:
                        if msg_type not in _message_types:
                            raise ProtocolError('Invalid message type: %d' % (msg_type, ))
                        handler = self.__handlers_locked[None]
                    handler(self, msg)
        except Exception as error:
            self.__cancel(error)
        else:
            self.__cancel()

    def __handle_greeting_locked(self, msg):
        if len(msg) < 11:
            raise ProtocolError('Incomplete greeting message')
        magic, = struct.unpack_from('>I', msg, 1)
        if magic != _greeting_magic:
            raise ProtocolError('Invalid magic number (0x%x != 0x%x)' % (magic, _greeting_magic))
        python_version = struct.unpack_from('BBB', msg, 5)
        if (python_version[0] >= 3) != (sys.version_info[0] >= 3):
            raise ProtocolError(
                'Remote python %r is not compatible with local %r' %
                (python_version, sys.version_info[:3]))
        self.__handlers_locked = self.__ready_handlers_locked
        self.__ready = True
        self.__ready_condition.notify_all()

    def __handle_greeting_bootstrap_locked(self, msg):
        if len(msg) < 5:
            raise ProtocolError('Incomplete greeting bootstrap message')
        magic, = struct.unpack_from('>I', msg, 1)
        if magic != _greeting_magic:
            raise ProtocolError('Invalid magic number (0x%x != 0x%x)' % (magic, _greeting_magic))

        from ._importer import get_bootstrap_line
        payload = get_bootstrap_line().encode('ascii')
        self.__enqueue_send_locked(payload)
        self.__handlers_locked = self.__ready_handlers_locked
        self.__ready = True
        self.__ready_condition.notify_all()

    def __handle_apply_locked(self, msg):
        if len(msg) < 9:
            raise ProtocolError('Incomplete apply message')
        actor_id, = struct.unpack_from('>Q', msg, 1)
        actor = self.__actors.get(actor_id)
        if actor is None:
            if not actor_id & 1:
                raise ProtocolError('Actor not found: %d' % (actor_id, ))
            actor = self.__actors[actor_id] = _Actor(actor_id)
            actor_thread = _ActorThread(actor, self.__tls)
            self.__actor_threads.append(actor_thread)
            actor_thread.start()
        actor.submit(self.__process_apply, msg, actor_id)

    def __process_apply(self, msg, actor_id):
        # TODO unhandled exceptions should cancel connection
        try:
            obj = self.__unpickle(msg[9:])
            with _ConnScope(self):
                try:
                    func, args, kwargs = obj
                    result = func(*args, **kwargs)
                except Exception as error:
                    obj = _dump_exception(error)
                    msg_type = _ERROR
                else:
                    obj = result
                    msg_type = _RESULT
            payload = self.__pickle(obj)
        except OperationError as error:
            payload = str(error).encode('utf-8', errors='replace')
            msg_type = _OPERATION_ERROR

        msg = struct.pack('>IBQ', len(payload) + 9, msg_type, actor_id ^ 1) + payload
        with self.__lock:
            if not self.__cancelled:
                self.__send_queue.append(msg)
                self.__send_condition.notify()
        return None

    def __handle_result_locked(self, msg):
        if len(msg) < 9:
            raise ProtocolError('Incomplete result message')
        actor_id, = struct.unpack_from('>Q', msg, 1)
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_result, msg)

    def __process_result(self, msg):
        try:
            return True, self.__unpickle(msg[9:])
        except OperationError as error:
            return False, error

    def __handle_error_locked(self, msg):
        if len(msg) < 9:
            raise ProtocolError('Incomplete error message')
        actor_id, = struct.unpack_from('>Q', msg, 1)
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_error, msg)

    def __process_error(self, msg):
        try:
            return False, _load_exception(self.__unpickle(msg[9:]))
        except OperationError as error:
            return False, error

    def __handle_operation_error_locked(self, msg):
        if len(msg) < 9:
            raise ProtocolError('Incomplete operation error message')
        actor_id, = struct.unpack_from('>Q', msg, 1)
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_operation_error, msg)

    def __process_operation_error(self, msg):
        return False, OperationError(msg[9:].decode('utf-8', errors='replace'))

    def __unexpected_in_ready_state(self, msg):
        raise ProtocolError('Unexpected message in ready state: %r' % (msg[0], ))

    def __unexpected_in_greeting_state(self, msg):
        raise ProtocolError('Unexpected message in greeting state: %r' % (msg[0], ))

    __ready_handlers_locked = {
        _APPLY: __handle_apply_locked,
        _RESULT: __handle_result_locked,
        _ERROR: __handle_error_locked,
        _OPERATION_ERROR: __handle_operation_error_locked,
        None: __unexpected_in_ready_state,
    }

    __greeting_handlers_locked = {
        _GREETING: __handle_greeting_locked,
        _GREETING_BOOTSTRAP: __handle_greeting_bootstrap_locked,
        None: __unexpected_in_greeting_state,
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
        return self.call(_apply_local, func, args[2:], kwargs)

    def replicate(self, obj):
        return self.call(Local, obj)


# based on PyErr_WriteUnraisable
def _ignore_exception_at(obj):
    try:
        print('Exception ignored in: {!r}'.format(obj), file=sys.stderr)
        traceback.print_exc()
    except:
        pass


def _shutdown():
    while _active_connections:
        conn_close = next(iter(_active_connections)).close
        try:
            conn_close()
        except:
            _ignore_exception_at(conn_close)


atexit.register(_shutdown)


def _load_exception(obj):
    if obj is None:
        return None
    exc = obj[0]
    exc.__context__ = _load_exception(obj[1])
    exc.__cause__ = _load_exception(obj[3])
    exc.__suppress_context__ = obj[2]
    exc.__traceback__ = _load_traceback(obj[4])
    return exc


def _dump_exception(exc):
    if exc is None:
        return None
    return (
        exc,
        _dump_exception(exc.__context__),
        exc.__suppress_context__,
        _dump_exception(exc.__cause__),
        _dump_traceback(exc.__traceback__),
    )


def _patched_code_name(code, name):
    # argument order is described in help(types.CodeType)
    return types.CodeType(
        code.co_argcount,
        code.co_kwonlyargcount,
        code.co_nlocals,
        code.co_stacksize,
        code.co_flags,
        code.co_code,
        code.co_consts,
        code.co_names,
        code.co_varnames,
        code.co_filename,
        name,
        code.co_firstlineno,
        code.co_lnotab,
        code.co_freevars,
        code.co_cellvars,
    )


def _dump_traceback(tb):
    result = []
    while tb is not None:
        code = tb.tb_frame.f_code
        result.append((code.co_filename, tb.tb_lineno, code.co_name))
        tb = tb.tb_next
    return result


def _load_traceback(obj):
    prev_var = '_e'
    globs = {prev_var: ZeroDivisionError}
    for i, (filename, lineno, name) in enumerate(reversed(obj)):
        this_var = '_%d' % i
        eval(compile('%sdef %s(): raise %s()' % ('\n' * (lineno - 1), this_var, prev_var), filename, 'exec'), globs)
        prev_var = this_var
        func = globs[this_var]
        func.__code__ = _patched_code_name(func.__code__, name)
    try:
        globs[prev_var]()
    except ZeroDivisionError:
        globs.clear()
        return sys.exc_info()[2].tb_next


# Synchronized queue implementation that can be safely used from within __del__ methods, as opposed to
# queue.Queue (https://bugs.python.org/issue14976). In Python 3.7, queue.SimpleQueue can be used instead.
#
# This class was written with CPython in mind, and may not work correctly with other Python implementations.
class _SimpleQueue:
    def __init__(self):
        self.__lock = threading.Lock()
        self.__waiter = threading.Lock()
        self.__items = collections.deque()

        # Create bound method objects for later use in get()/put() - creating such objects may trigger GC,
        # which must not happen inside get()/put(). These methods are all written in C, and are expected not to
        # allocate GC memory internally (e.g. with PyObject_GC_New).
        self.__lock_acquire = self.__lock.acquire
        self.__lock_release = self.__lock.release
        self.__waiter_acquire = self.__waiter.acquire
        self.__waiter_release = self.__waiter.release
        self.__items_popleft = self.__items.popleft
        self.__items_append = self.__items.append

        self.__waiter_acquire()

    # Wait until a queue becomes non-empty, and retrieve a single element from the queue. This function may be
    # called only from a single thread at a time.
    def get(self):
        self.__waiter_acquire()
        self.__lock_acquire()
        item = self.__items_popleft()
        if self.__items:
            self.__waiter_release()
        self.__lock_release()
        return item

    # Insert a single element at the end of the queue. This method may be safely called from multiple threads,
    # and from __del__ methods.
    def put(self, item):
        self.__lock_acquire()
        if not self.__items:
            self.__waiter_release()
        self.__items_append(item)
        self.__lock_release()


# Class responsible for executing arbitrary functions on a dedicated worker thread.
class _ElsewhereExecutor:
    def __init__(self):
        self.__queue = _SimpleQueue()
        self.__startable = True
        self.__lock = threading.Lock()
        self.__thread = None
        self.__closing = False

    # Execute a given function on a worker thread. This method may be safely used from __del__ methods. If shutdown()
    # method is called, there is no guarantee that submitted functions get executed.
    def submit(self, func, *args):
        self.__queue.put((func, args))
        if self.__startable:
            self.__startable = False
            # Important: below code may trigger GC, which in turn may call submit() method again. If submit()
            # is called again, we must not reach this point. This is why self.__startable guard is necessary.
            with self.__lock:
                if self.__thread is None and not self.__closing:
                    self.__thread = threading.Thread(target=self.__thread_main, daemon=True)
                    self.__thread.start()

    # Stop worker thread. This method must be explicitly called to avoid unexpected errors during interpreter shutdown.
    def shutdown(self):
        self.__queue.put(None)
        self.__startable = False
        with self.__lock:
            # Important: below code may trigger GC, which in turn may call submit() method. Is is vital that
            # submit() does not try to acquire lock again - this is why self.__startable is set to False before
            # acquriring self.__lock.
            self.__closing = True
            if self.__thread is not None:
                self.__thread.join()
                self.__thread = None

    def __thread_main(self):
        for func, args in iter(self.__queue.get, None):
            try:
                func(*args)
            except:
                _ignore_exception_at(func)


_elsewhere_executor = _ElsewhereExecutor()
atexit.register(_elsewhere_executor.shutdown)

_call_elsewhere = _elsewhere_executor.submit


def _call_del_obj(conn, obj_id):
    try:
        conn.call(_del_obj, obj_id)
    except DisconnectError:
        pass


class _BootstrappedConnection(Connection):
    def __init__(self, channel, finder):
        finder.set_connection(self)  # must be done before starting communication threads
        super(_BootstrappedConnection, self).__init__(channel)


class Proxy(object):
    def __init__(self, id):
        self._Proxy__conn = get_connection()
        self.__id = id

    def __deepcopy__(self, memo):
        return self._Proxy__conn.call(_get_obj, self.__id)

    def __reduce__(self):
        if not _conn_tls.pickling:
            raise RuntimeError("Only 'across.Connection' objects can pickle 'across.Proxy' objects")
        conn = _conn_tls.conn
        if conn is not self._Proxy__conn:
            raise RuntimeError("Proxy {!r} can only be pickled by {!r} connection, not {!r}".format(
                self, self._Proxy__conn, conn))
        return _get_obj, (self.__id, )

    def __del__(self):
        _call_elsewhere(_call_del_obj, self._Proxy__conn, self.__id)


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
            '        return self._Proxy__conn.call(_apply_method, self, %(name)r, args, kwargs)\n'
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
        cls = type(self.value)
        if not _conn_tls.pickling:
            raise RuntimeError("Only 'across.Connection' objects can pickle 'across.Local' objects")
        return _make_proxy, (_types_to_names.get(cls, cls), _conn_tls.conn._put_obj(self.value))


_names_to_types = {
    'func': types.FunctionType,
    'builtin_func': types.BuiltinFunctionType,
}
_types_to_names = dict((v, k) for k, v in _names_to_types.items())


def _apply_method(obj, name, args, kwargs):
    return getattr(obj, name)(*args, **kwargs)


def _get_obj(id):
    return get_connection()._get_obj(id)


def _del_obj(id):
    return get_connection()._del_obj(id)


def _apply_local(func, args, kwargs):
    return Local(func(*args, **kwargs))


def _make_proxy(cls, id):
    cls = _names_to_types.get(cls, cls)
    proxy_type = _proxy_types.get(cls)
    if proxy_type is None:
        proxy_type = _proxy_types[cls] = _make_auto_proxy_type(cls)
    return proxy_type(id)


def get_connection():
    conn = _conn_tls.conn
    if conn is None:
        raise RuntimeError
    return conn
