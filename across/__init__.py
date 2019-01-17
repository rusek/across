import threading
import collections
import struct
import pickle
import sys
import types
import atexit
import traceback
import io
import queue
import ast


_version = (0, 1, 0)
__version__ = '%d.%d.%d' % _version


class _Framer:
    def __init__(self, channel):
        self.__channel = channel

    def send_superblock(self, superblock):
        self.__sendall(superblock)

    def send_frame(self, frame):
        self.__sendall(struct.pack('>I', len(frame)) + frame)

    def __sendall(self, data):
        while data:
            size = self.__channel.send(data)
            assert size > 0
            data = data[size:]

    def recv_superblock(self, size):
        superblock = self.__recvall(size)
        if len(superblock) != size:
            raise ProtocolError('Incomplete superblock')
        return superblock

    def recv_frame(self):
        header = self.__recvall(4)
        if len(header) != 4:
            raise ProtocolError('Incomplete frame size')
        size, = struct.unpack('>I', header)
        frame = self.__recvall(size)
        if len(frame) != size:
            raise ProtocolError('Incomplete frame')
        return frame

    def __recvall(self, size):
        data = b''
        while size:
            chunk = self.__channel.recv(size)
            if not chunk:
                return data
            data += chunk
            size -= len(chunk)
        return data


class _Message:
    def __init__(self, frame=None):
        self.__buffer = io.BytesIO(frame)

    def as_bytes(self):
        return self.__buffer.getvalue()

    def put_uint(self, obj):
        if obj <= 0xfc:
            self.__buffer.write(struct.pack('>B', obj))
        elif obj <= 0xffff:
            self.__buffer.write(b'\xfd' + struct.pack('>H', obj))
        elif obj <= 0xffffffff:
            self.__buffer.write(b'\xfe' + struct.pack('>I', obj))
        else:
            self.__buffer.write(b'\xff' + struct.pack('>Q', obj))

    def get_uint(self):
        data = self.__buffer.read(1)
        if not data:
            return 0
        obj, = struct.unpack('>B', data)
        if obj <= 0xfc:
            return obj
        elif obj == 0xfd:
            return struct.unpack('>H', self.__buffer.read(2))[0]
        elif obj == 0xfe:
            return struct.unpack('>I', self.__buffer.read(4))[0]
        else:
            return struct.unpack('>Q', self.__buffer.read(8))[0]

    def put_bytes(self, obj):
        self.put_uint(len(obj))
        self.__buffer.write(obj)

    def get_bytes(self):
        return self.__buffer.read(self.get_uint())


class _ConnTls(threading.local):
    def __init__(self):
        self.conn = None
        self.proxy_ids = None


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
_GOODBYE = 5
_DEL_OBJS = 6
_IDLE = 7


class _ConnScope(object):
    def __init__(self, conn, proxy_ids=None):
        self.__conn = conn
        self.__proxy_ids = proxy_ids

    def __enter__(self):
        _conn_tls.conn, self.__conn = self.__conn,  _conn_tls.conn
        _conn_tls.proxy_ids, self.__proxy_ids = self.__proxy_ids, _conn_tls.proxy_ids

    def __exit__(self, exc_type, exc_val, exc_tb):
        _conn_tls.conn = self.__conn
        _conn_tls.proxy_ids = self.__proxy_ids


class _SenderThread(threading.Thread):
    def __init__(self, framer, cancel_func):
        super(_SenderThread, self).__init__(daemon=True)
        self.__framer = framer
        self.__cancel_func = cancel_func
        self.__queue = _SimpleQueue()
        self.__idle_timeout = None

    def run(self):
        try:
            while True:
                try:
                    task = self.__queue.get(self.__idle_timeout)
                except queue.Empty:
                    task = self.__idle
                if task() is False:
                    break
        except Exception as error:
            self.__cancel_func(error)

    def __idle(self):
        msg = _Message()
        msg.put_uint(_IDLE)
        self.__framer.send_frame(msg.as_bytes())

    def send_superblock(self, superblock):
        def handler():
            self.__framer.send_superblock(superblock)

        self.__queue.put(handler)

    # Important: this function must be reentrant
    def send_frame(self, frame):
        def handler():
            self.__framer.send_frame(frame)

        self.__queue.put(handler)

    def send_frame_and_stop(self, frame):
        def handler():
            self.__framer.send_frame(frame)
            return False

        self.__queue.put(handler)

    def stop(self):
        def handler():
            return False

        self.__queue.put(handler)

    def update_idle_timeout(self, timeout):
        def handler():
            self.__idle_timeout = timeout

        self.__queue.put(handler)


_SUPERBLOCK_SIZE = 16
_MAGIC = 0xe35b9e78

_OS = 0
_BIOS = 1


def _get_superblock():
    return struct.pack('>IB', _MAGIC, _OS) + b'\0' * (_SUPERBLOCK_SIZE - 5)


def get_bios():
    to_skip = _SUPERBLOCK_SIZE + 4
    to_send = struct.pack('>IB', _MAGIC, _BIOS) + b'\0' * (_SUPERBLOCK_SIZE - 5)
    return ("import sys;i,o=sys.stdin.buffer,sys.stdout.buffer;o.write(%r);o.flush();i.read(%r);"
            "exec(i.readline())" % (to_send, to_skip))


def _get_greeting_frame(timeout_ms):
    msg = _Message()
    msg.put_uint(_GREETING)
    for num in sys.version_info[:3] + _version:
        msg.put_uint(num)
    msg.put_uint(timeout_ms)
    return msg.as_bytes()


# timeout (in ms) is transmitted as uint64; these limits help avoiding internal
# errors when too small/large value is given; note that 1ms timeout will probably
# sooner or later cause a connection loss
_MIN_TIMEOUT = 0.001
_MAX_TIMEOUT = 365 * 24 * 60 * 60


def _sanitize_timeout(timeout):
    if timeout is None:
        return timeout
    if isinstance(timeout, int):
        timeout = float(timeout)
    elif not isinstance(timeout, float):
        raise TypeError('timeout must be a float')
    if not timeout > 0.0:
        raise ValueError('timeout must be positive')
    return max(_MIN_TIMEOUT, min(_MAX_TIMEOUT, timeout))


_unclosed_connections = set()

# Receiver thread establishes connection / performs handshake. Thre is no error (Connection.__cancel_error is None).
# Sending data is allowed only from receiver thread.
_STARTING = 0
# Handshake completed. No error. Any thread may send data.
_RUNNING = 1
# Receiver thread shuts down connection. Sender thread no longer accepts new data to send (new data is silently
# dropped). Some error might have occurred (Connection.__cancel_error may not be None).
_STOPPING = 2
# Receiver thread finished execution and is waiting to be joined. All other resources are already freed.
# Some error might have occurred.
_STOPPED = 3
# Receiver thread was joined. Error, if any, has been re-raised to the user (from Connection.close()).
_CLOSED = 4


class Connection:
    def __init__(self, channel, timeout=None):
        timeout = _sanitize_timeout(timeout)
        if timeout is not None:
            channel.set_timeout(timeout)
            timeout_ms = max(1, int(round(timeout * 1000.0)))
        else:
            timeout_ms = 0
        self.__channel = channel
        self.__timeout_ms = timeout_ms
        self.__framer = _Framer(channel)
        self.__lock = threading.Lock()
        self.__sender = _SenderThread(self.__framer, cancel_func=self.__cancel)
        self.__receiver_thread = threading.Thread(target=self.__receiver_loop, daemon=True)
        self.__state_condition = threading.Condition(self.__lock)
        self.__cancel_error = None
        self.__handlers_locked = self.__greeting_handlers_locked
        self.__state = _STARTING
        self.__sender.send_superblock(_get_superblock())

        self.__actors = {}
        self.__next_actor_id = 0
        self.__tls = threading.local()
        self.__actor_threads = []

        self.__objs = {}
        self.__obj_counter = 0

        self.__receiver_thread.start()

        _unclosed_connections.add(self)

    def _get_obj(self, id):
        return self.__objs[id]

    def _put_obj(self, obj):
        with self.__lock:
            id = self.__obj_counter
            self.__obj_counter += 1
        self.__objs[id] = obj
        return id

    # Important: this function must be reentrant
    def _del_remote_obj(self, id):
        msg = _Message()
        msg.put_uint(_DEL_OBJS)
        msg.put_uint(1)
        msg.put_uint(id)
        self.__sender.send_frame(msg.as_bytes())

    def __enter__(self):
        with self.__lock:
            while self.__state in (_STARTING, _STOPPING):
                self.__state_condition.wait()
            if self.__state != _RUNNING:
                self.__set_closed_locked()
                raise ValueError('Connection is closed')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        with self.__lock:
            while self.__state == _STARTING:
                self.__state_condition.wait()
            self.__set_stopping_locked()
            while self.__state == _STOPPING:
                self.__state_condition.wait()
            self.__set_closed_locked()

    def __set_closed_locked(self):
        if self.__state != _CLOSED:
            self.__state = _CLOSED
            _unclosed_connections.remove(self)
            self.__receiver_thread.join()
            if self.__cancel_error is not None:
                try:
                    raise self.__cancel_error
                finally:
                    self.__cancel_error = None

    def cancel(self):
        self.__cancel(CancelledError())

    def __cancel(self, error):
        with self.__lock:
            if self.__cancel_error is None and self.__state not in (_STOPPED, _CLOSED):
                self.__cancel_error = error
                self.__set_stopping_locked()
                try:
                    self.__channel.cancel()
                except Exception:
                    _ignore_exception_at(self.__channel)

    def __set_stopping_locked(self):
        if self.__state in (_STARTING, _RUNNING):
            self.__state = _STOPPING
            self.__state_condition.notify_all()
            if self.__cancel_error is None:
                msg = _Message()
                msg.put_uint(_GOODBYE)
                self.__sender.send_frame_and_stop(msg.as_bytes())
            else:
                self.__sender.stop()
            for actor in self.__actors.values():
                actor.cancel()

    def __get_current_actor_locked(self):
        try:
            return self.__tls.actor
        except AttributeError:
            actor_id = self.__next_actor_id
            self.__next_actor_id += 2
            actor = self.__actors[actor_id] = self.__tls.actor = _Actor(actor_id)
            return actor

    def call(*args, **kwargs):
        try:
            (self, func), args = args[:2], args[2:]
        except ValueError:
            if not args:
                msg = "call() missing 2 required positional arguments: 'self' and 'func'"
            else:
                msg = "call() missing 1 required positional argument: 'func'"
            raise TypeError(msg) from None

        with self.__lock:
            while self.__state == _STARTING:
                self.__state_condition.wait()
            if self.__state != _RUNNING:
                raise DisconnectError
            actor = self.__get_current_actor_locked()

        msg = _Message()
        msg.put_uint(_APPLY)
        msg.put_uint(actor.id ^ 1)
        self.__serialize(msg, (func, args, kwargs))
        self.__sender.send_frame(msg.as_bytes())
        success, value = actor.run()
        if success:
            return value
        else:
            raise value

    def __serialize(self, msg, obj):
        proxy_ids = []
        with _ConnScope(self, proxy_ids=proxy_ids):
            try:
                msg.put_bytes(pickle.dumps(obj))
                msg.put_uint(len(proxy_ids))
                for proxy_id in proxy_ids:
                    msg.put_uint(proxy_id)
            except Exception as error:
                for proxy_id in proxy_ids:
                    del self.__objs[proxy_id]
                raise OperationError('Pickling failed: %s' % (error, ))

    def __deserialize(self, msg):
        proxy_ids = []
        with _ConnScope(self, proxy_ids=proxy_ids):
            try:
                return pickle.loads(msg.get_bytes())
            except Exception as error:
                leaked_proxy_ids = set(msg.get_uint() for _ in range(msg.get_uint())) - set(proxy_ids)
                if leaked_proxy_ids:
                    msg = _Message()
                    msg.put_uint(_DEL_OBJS)
                    msg.put_uint(len(leaked_proxy_ids))
                    for proxy_id in leaked_proxy_ids:
                        msg.put_uint(proxy_id)
                    self.__sender.send_frame(msg.as_bytes())
                raise OperationError('Unpickling failed: %s' % (error, ))

    def __receiver_loop(self):
        try:
            self.__channel.connect()
        except Exception as error:
            self.__cancel(error)
        else:
            self.__sender.start()

            try:
                self.__receive_superblock()
                self.__receive_msgs()
            except Exception as error:
                self.__cancel(error)

            self.__sender.join()

            try:
                self.__channel.close()
            except Exception as error:
                self.__cancel(error)

            for actor_thread in self.__actor_threads:
                actor_thread.join()

        with self.__lock:
            self.__state = _STOPPED
            self.__state_condition.notify_all()

    def __receive_superblock(self):
        while True:
            superblock = self.__framer.recv_superblock(_SUPERBLOCK_SIZE)
            magic, mode = struct.unpack_from('>IB', superblock)
            if _MAGIC != magic:
                raise ProtocolError('Invalid magic: 0x%x' % (magic, ))
            if mode == _OS:
                self.__sender.send_frame(_get_greeting_frame(self.__timeout_ms))
                break
            elif mode == _BIOS:
                from ._importer import get_bootstrap_line
                payload = get_bootstrap_line().encode('ascii')
                self.__sender.send_frame(payload)
                self.__sender.send_superblock(_get_superblock())
            else:
                raise ProtocolError('Invalid mode: %d' % (mode, ))

    def __receive_msgs(self):
        while True:
            msg = _Message(self.__framer.recv_frame())
            with self.__lock:
                if self.__cancel_error is not None:
                    break
                msg_type = msg.get_uint()
                handler = self.__handlers_locked.get(msg_type)
                if handler is None:
                    handlers_name = self.__handlers_locked[None]
                    raise ProtocolError('Invalid message in %s state: %d' % (handlers_name, msg_type))
                if handler(self, msg) is False:
                    break

    def __handle_greeting_locked(self, msg):
        python_version = (msg.get_uint(), msg.get_uint(), msg.get_uint())
        across_version = (msg.get_uint(), msg.get_uint(), msg.get_uint())  # unused for now
        timeout_ms = msg.get_uint()
        if (python_version[0] >= 3) != (sys.version_info[0] >= 3):
            raise ProtocolError(
                'Remote python %r is not compatible with local %r' %
                (python_version, sys.version_info[:3]))
        if timeout_ms:
            # idle messages should be sent after a half of timeout passes
            idle_timeout = timeout_ms / 2000.0
        else:
            idle_timeout = None

        self.__sender.update_idle_timeout(idle_timeout)
        self.__handlers_locked = self.__ready_handlers_locked
        self.__state = _RUNNING
        self.__state_condition.notify_all()

    def __handle_apply_locked(self, msg):
        if self.__state != _RUNNING:
            return
        actor_id = msg.get_uint()
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
            obj = self.__deserialize(msg)
            with _ConnScope(self):
                try:
                    func, args, kwargs = obj
                    result = func(*args, **kwargs)
                except Exception as error:
                    msg = _Message()
                    msg.put_uint(_ERROR)
                    msg.put_uint(actor_id ^ 1)
                    self.__serialize(msg, _dump_exception(error))
                else:
                    msg = _Message()
                    msg.put_uint(_RESULT)
                    msg.put_uint(actor_id ^ 1)
                    self.__serialize(msg, result)
        except OperationError as error:
            msg = _Message()
            msg.put_uint(_OPERATION_ERROR)
            msg.put_uint(actor_id ^ 1)
            msg.put_bytes(str(error).encode('utf-8', errors='replace'))

        self.__sender.send_frame(msg.as_bytes())
        return None

    def __handle_result_locked(self, msg):
        actor_id = msg.get_uint()
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_result, msg)

    def __process_result(self, msg):
        try:
            return True, self.__deserialize(msg)
        except OperationError as error:
            return False, error

    def __handle_error_locked(self, msg):
        actor_id = msg.get_uint()
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_error, msg)

    def __process_error(self, msg):
        try:
            return False, _load_exception(self.__deserialize(msg))
        except OperationError as error:
            return False, error

    def __handle_operation_error_locked(self, msg):
        actor_id = msg.get_uint()
        actor = self.__actors.get(actor_id)
        if actor is None:
            raise ProtocolError('Actor not found: %d' % (actor_id, ))
        actor.submit(self.__process_operation_error, msg)

    def __process_operation_error(self, msg):
        return False, OperationError(msg.get_bytes().decode('utf-8', errors='replace'))

    def __handle_del_objs_locked(self, msg):
        for _ in range(msg.get_uint()):
            del self.__objs[msg.get_uint()]

    def __handle_goodbye_locked(self, msg):
        self.__set_stopping_locked()
        return False

    def __handle_idle_locked(self, msg):
        pass

    __ready_handlers_locked = {
        _APPLY: __handle_apply_locked,
        _RESULT: __handle_result_locked,
        _ERROR: __handle_error_locked,
        _OPERATION_ERROR: __handle_operation_error_locked,
        _DEL_OBJS: __handle_del_objs_locked,
        _GOODBYE: __handle_goodbye_locked,
        _IDLE: __handle_idle_locked,
        None: 'ready',
    }

    __greeting_handlers_locked = {
        _GREETING: __handle_greeting_locked,
        None: 'greeting',
    }

    def wait(self):
        with self.__lock:
            while self.__state in (_STARTING, _RUNNING):
                self.__state_condition.wait()

    def create(*args, **kwargs):
        try:
            self, func = args[:2]
        except ValueError:
            if not args:
                msg = "create() missing 2 required positional arguments: 'self' and 'func'"
            else:
                msg = "create() missing 1 required positional argument: 'func'"
            raise TypeError(msg) from None
        return self.call(_apply_ref, func, args[2:], kwargs)

    def replicate(self, obj):
        return self.call(ref, obj)

    def execute(*args, **kwargs):
        try:
            self, source = args[:2]
        except ValueError:
            if not args:
                msg = "execute() missing 2 required positional arguments: 'self' and 'source'"
            else:
                msg = "execute() missing 1 required positional argument: 'source'"
            raise TypeError(msg) from None
        return self.call(_execute, source, kwargs)


# based on PyErr_WriteUnraisable
def _ignore_exception_at(obj):
    try:
        print('Exception ignored in: {!r}'.format(obj), file=sys.stderr)
        traceback.print_exc()
    except:
        pass


def _shutdown():
    while _unclosed_connections:
        conn_close = next(iter(_unclosed_connections)).close
        try:
            conn_close()
        except:
            _ignore_exception_at(conn_close)


atexit.register(_shutdown)


def _execute(source, scope):
    filename = '<execute>'
    varname = '__across_ret'
    mod = compile(source, filename, 'exec', ast.PyCF_ONLY_AST)
    if mod.body and isinstance(mod.body[-1], ast.Expr):
        mod.body[-1] = ast.Assign(
            [ast.Name(varname, ast.Store(), lineno=0, col_offset=0)],
            mod.body[-1].value, lineno=0, col_offset=0)
    exec(compile(mod, filename, 'exec'), scope)
    return scope.get(varname)


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
    def get(self, timeout=None):
        if timeout is None:
            self.__waiter_acquire()
        elif not self.__waiter_acquire(timeout=timeout):
            raise queue.Empty
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
        if _conn_tls.proxy_ids is None:
            raise RuntimeError("Only 'across.Connection' objects can pickle 'across.Proxy' objects")
        conn = _conn_tls.conn
        if conn is not self._Proxy__conn:
            raise RuntimeError("Proxy {!r} can only be pickled by {!r} connection, not {!r}".format(
                self, self._Proxy__conn, conn))
        return _get_obj, (self.__id, )

    def __del__(self):
        self._Proxy__conn._del_remote_obj(self.__id)


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


class Reference(object):
    def __init__(self, value):
        assert not isinstance(value, (Proxy, Reference))
        self.value = value

    def __reduce__(self):
        cls = type(self.value)
        if _conn_tls.proxy_ids is None:
            raise RuntimeError("Only 'across.Connection' objects can pickle 'across.Reference' objects")
        proxy_id = _conn_tls.conn._put_obj(self.value)
        _conn_tls.proxy_ids.append(proxy_id)
        return _make_proxy, (_types_to_names.get(cls, cls), proxy_id)


def ref(obj):
    return Reference(obj)


_names_to_types = {
    'func': types.FunctionType,
    'builtin_func': types.BuiltinFunctionType,
}
_types_to_names = dict((v, k) for k, v in _names_to_types.items())


def _apply_method(obj, name, args, kwargs):
    return getattr(obj, name)(*args, **kwargs)


def _get_obj(id):
    return get_connection()._get_obj(id)


def _apply_ref(func, args, kwargs):
    return ref(func(*args, **kwargs))


def _make_proxy(cls, id):
    cls = _names_to_types.get(cls, cls)
    proxy_type = _proxy_types.get(cls)
    if proxy_type is None:
        proxy_type = _proxy_types[cls] = _make_auto_proxy_type(cls)
    _conn_tls.proxy_ids.append(id)
    return proxy_type(id)


def get_connection():
    conn = _conn_tls.conn
    if conn is None:
        raise RuntimeError
    return conn
