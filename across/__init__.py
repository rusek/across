import threading
import struct
import pickle
import sys
import types
import atexit
import io
import queue
import ast
import socket
import linecache
import os
import warnings
import copy
import errno

from ._importer import get_bootstrap_line, take_finder
from ._utils import (
    ignore_exception_at, Executor, Future, SimpleQueue, format_exception_only, logger as _logger,
    IdentityAdapter, atomic_count, set_debug_level)
from ._channels import PipeChannel, SocketChannel, ProcessChannel


if sys.version_info < (3, 4):
    warnings.warn('Python versions older than 3.4 are not supported')


_version = (0, 1, 0)
__version__ = '{}.{}.{}'.format(*_version)


class _Framer:
    def __init__(self, channel):
        self.__channel = channel

    def send_superblock(self, superblock):
        self.__sendall(superblock)

    def send_frame(self, frame):
        self.__sendall(struct.pack('>I', len(frame)) + frame)

    def __sendall(self, data):
        buffer = memoryview(data)
        while buffer:
            size = self.__channel.send(buffer)
            assert size > 0
            buffer = buffer[size:]

    def recv_superblock(self, size):
        superblock = self.__recvall(size)
        if superblock is None:
            raise EOFError('Incomplete superblock')
        return superblock

    def recv_frame(self):
        header = self.__recvall(4)
        if header is None:
            raise EOFError('Incomplete frame size')
        size, = struct.unpack('>I', header)
        frame = self.__recvall(size)
        if frame is None:
            raise EOFError('Incomplete frame')
        return frame

    def __recvall(self, size):
        data = bytearray(size)
        buffer = memoryview(data)
        while buffer:
            size = self.__channel.recv_into(buffer)
            if size == 0:
                return None
            buffer = buffer[size:]
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


class OperationError(Exception):
    pass


class DisconnectError(OperationError):
    pass


class ProtocolError(Exception):
    pass


_GREETING = 0
_APPLY = 1
_RESULT = 2
_GOODBYE = 3
_DEL_OBJS = 4
_IDLE = 5

_RESULT_SUCCESS = 2
_RESULT_ERROR = 3
_RESULT_OPERATION_ERROR = 4


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


_SenderQueue = SimpleQueue  # patched by tests


class _SenderThread(threading.Thread):
    def __init__(self, framer, cancel_func, logger):
        super(_SenderThread, self).__init__(daemon=True)
        self.__logger = logger
        self.__framer = framer
        self.__cancel_func = cancel_func
        self.__queue = _SenderQueue()
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
        self.__logger.debug('Sending idle frame')
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
_BIOS_MAGIC = 0xe54e6d01


def _get_superblock():
    return struct.pack('>IBBB', _MAGIC, *_version) + b'\0' * (_SUPERBLOCK_SIZE - 7)


def _get_bios_superblock():
    return struct.pack('>IBBB', _BIOS_MAGIC, 0, 1, 0) + b'\0' * (_SUPERBLOCK_SIZE - 7)


def get_bios():
    to_skip = _SUPERBLOCK_SIZE + 4
    to_send = _get_bios_superblock()
    return ("import sys;i,o=sys.stdin.buffer,sys.stdout.buffer;o.write({!r});o.flush();i.read({!r});"
            "ACROSS='stdio',;exec(i.readline())".format(to_send, to_skip))


def _get_greeting_frame(timeout_ms):
    msg = _Message()
    msg.put_uint(_GREETING)
    msg.put_uint(timeout_ms)
    return msg.as_bytes()


# timeout (in ms) is transmitted as uint64; these limits help avoiding internal
# errors when too small/large value is given; note that 1ms timeout will probably
# sooner or later cause a connection loss; we also have to ensure that system limits
# are not exceeded; e.g. threading.TIMEOUT_MAX is around 49 days on Windows
_DEFAULT_TIMEOUT = 60.0
_MIN_TIMEOUT = 0.001
_MAX_TIMEOUT = 86400.0  # one day


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


class Options:
    _check_mode = False

    def __init__(self, **options):
        self.timeout = _DEFAULT_TIMEOUT

        self._assign(options)

    def copy(self, **options):
        other = copy.copy(self)
        other._assign(options)
        return other

    def _assign(self, options):
        for name, value in options.items():
            setattr(self, name, value)

    # Let's be nice enough to detect typos when setting options (e.g. 'timout')
    def __setattr__(self, name, value):
        if Options._check_mode and not hasattr(_default_options, name):
            raise AttributeError('No option named \'{}\''.format(name))
        super().__setattr__(name, value)

    def __repr__(self):
        options = []
        for option, value in sorted(self.__dict__.items()):
            if value != getattr(_default_options, option):
                options.append('{}={!r}'.format(option, value))
        return '{}({})'.format(self.__class__.__name__, ', '.join(options))


_default_options = Options()
Options._check_mode = True


_unclosed_connections = set()
_connection_counter = atomic_count(1)

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
    def __init__(self, channel, *, options=None, on_stopped=None, logger=_logger):
        if options is None:
            options = _default_options
        timeout = _sanitize_timeout(options.timeout)
        if timeout is not None:
            channel.set_timeout(timeout)
            timeout_ms = max(1, int(round(timeout * 1000.0)))
        else:
            timeout_ms = 0

        finder = take_finder()
        if finder:
            finder.set_connection(self)

        self.__logger = logger
        self.__channel = channel
        self.__timeout_ms = timeout_ms
        self.__framer = _Framer(channel)
        self.__lock = threading.Lock()
        self.__sender = _SenderThread(self.__framer, logger=self.__logger, cancel_func=self.__cancel)
        self.__receiver_thread = threading.Thread(target=self.__receiver_loop, daemon=True)
        self.__state_condition = threading.Condition(self.__lock)
        self.__cancel_error = None
        self.__handlers_locked = self.__greeting_handlers_locked
        self.__state = _STARTING
        self.__was_running = False
        self.__on_stopped = on_stopped
        self.__sender.send_superblock(_get_superblock())

        self.__calls = {}
        self.__next_call_id = 0
        self.__executor = Executor()

        self.__objs = {}
        self.__obj_counter = 0

        self._finder = finder

        _unclosed_connections.add(self)

        self.__logger.info('Connection starts, channel=%r, options=%r', channel, options)
        self.__receiver_thread.start()

    @classmethod
    def _make_logger(cls):
        return IdentityAdapter(_logger, 'conn/{}'.format(next(_connection_counter)))

    @classmethod
    def from_tcp(cls, host, port, **kwargs):
        logger = cls._make_logger()
        return cls(
            SocketChannel(family=socket.AF_UNSPEC, address=(host, port), resolve=True, logger=logger),
            logger=logger,
            **kwargs
        )

    @classmethod
    def from_unix(cls, path, **kwargs):
        if not hasattr(socket, 'AF_UNIX'):
            raise RuntimeError('Unix domain sockets are not available')
        logger = cls._make_logger()
        return cls(
            SocketChannel(family=socket.AF_UNIX, address=path, logger=logger),
            logger=logger,
            **kwargs
        )

    @classmethod
    def from_socket(cls, sock, **kwargs):
        logger = cls._make_logger()
        return cls(SocketChannel(sock=sock, logger=logger), logger=logger, **kwargs)

    @classmethod
    def from_pipes(cls, in_pipe, out_pipe, **kwargs):
        logger = cls._make_logger()
        return cls(PipeChannel(in_pipe, out_pipe, close=True, logger=logger), logger=logger, **kwargs)

    @classmethod
    def from_stdio(cls, **kwargs):
        logger = cls._make_logger()
        return cls(
            PipeChannel(sys.stdin.buffer, sys.stdout.buffer, close=False, logger=logger),
            logger=logger,
            **kwargs
        )

    @classmethod
    def from_command(cls, args, **kwargs):
        assert isinstance(args, (list, tuple))
        logger = cls._make_logger()
        return cls(ProcessChannel(args, logger=logger), logger=logger, **kwargs)

    @classmethod
    def from_shell(cls, script, **kwargs):
        assert isinstance(script, (str, bytes))
        logger = cls._make_logger()
        return cls(ProcessChannel(script, shell=True, logger=logger), logger=logger, **kwargs)

    @classmethod
    def from_process(cls, proc, **kwargs):
        logger = cls._make_logger()
        return cls(ProcessChannel(proc=proc, logger=logger), logger=logger, **kwargs)

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
            while self.__state == _STARTING or (self.__state == _STOPPING and not self.__was_running):
                self.__state_condition.wait()
            if self.__was_running and self.__state != _CLOSED:
                return self
            if self.__state != _RUNNING:
                self.__set_closed_locked()
                raise ValueError('Connection is closed')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def export(self, *modules):
        self.call(_export, modules)

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
            self.__logger.info('Connection closed')
            if self.__cancel_error is not None:
                try:
                    raise self.__cancel_error
                finally:
                    self.__cancel_error = None

    def cancel(self):
        self.__cancel(None)

    def __cancel(self, error):
        with self.__lock:
            if self.__cancel_error is None and self.__state not in (_STOPPED, _CLOSED):
                if error is None:
                    self.__logger.info('Cancelling connection')
                    error = OSError(errno.ECANCELED, os.strerror(errno.ECANCELED))
                else:
                    self.__logger.error('Aborting connection due to %r', error)
                self.__cancel_error = error
                self.__set_stopping_locked()
                try:
                    self.__channel.cancel()
                except Exception:
                    ignore_exception_at(self.__channel)

    def __set_stopping_locked(self):
        if self.__state in (_STARTING, _RUNNING):
            self.__state = _STOPPING
            self.__state_condition.notify_all()
            if self.__cancel_error is None:
                self.__logger.info('Stopping connection')
                msg = _Message()
                msg.put_uint(_GOODBYE)
                self.__sender.send_frame_and_stop(msg.as_bytes())
            else:
                self.__sender.stop()
            for future in self.__calls.values():
                future.set_exception(self.__make_disconnect_error_locked())
            self.__calls.clear()

    def call(_self, _func, *args, **kwargs):
        future = Future()

        with _self.__lock:
            while _self.__state == _STARTING:
                _self.__state_condition.wait()
            if _self.__state != _RUNNING:
                raise _self.__make_disconnect_error_locked()
            call_id = _self.__next_call_id
            _self.__next_call_id += 1
            _self.__calls[call_id] = future

        try:
            msg = _Message()
            msg.put_uint(_APPLY)
            msg.put_uint(call_id)
            _self.__serialize(msg, (_func, args, kwargs))
        except BaseException:
            with _self.__lock:
                # May be already deleted if connection entered stopping state
                _self.__calls.pop(call_id, None)
            raise

        _self.__sender.send_frame(msg.as_bytes())

        msg = future.result()
        result = msg.get_uint()
        if result == _RESULT_SUCCESS:
            return _self.__deserialize(msg)
        elif result == _RESULT_ERROR:
            raise _self.__deserialize(msg, as_exc=True)
        else:  # _RESULT_OPERATION_ERROR
            raise OperationError(msg.get_bytes().decode('utf-8', errors='replace'))

    def __make_disconnect_error_locked(self):
        if self.__state == _CLOSED:
            return DisconnectError('Connection is closed')
        elif self.__cancel_error is None:
            return DisconnectError('Connection was remotely closed')
        else:
            return DisconnectError('Connection was aborted due to {}'.format(
                format_exception_only(self.__cancel_error)))

    def __serialize(self, msg, obj, as_exc=False):
        proxy_ids = []
        with _ConnScope(self, proxy_ids=proxy_ids):
            try:
                if as_exc:
                    obj = _dump_exception(obj)
                # Use pickle protocol added in Python 3.4.
                msg.put_bytes(pickle.dumps(obj, protocol=4))
                msg.put_uint(len(proxy_ids))
                for proxy_id in proxy_ids:
                    msg.put_uint(proxy_id)
            except Exception as error:
                for proxy_id in proxy_ids:
                    del self.__objs[proxy_id]
                raise OperationError('Pickling failed due to {}'.format(
                    format_exception_only(error))) from error

    def __deserialize(self, msg, as_exc=False):
        proxy_ids = []
        with _ConnScope(self, proxy_ids=proxy_ids):
            try:
                obj = pickle.loads(msg.get_bytes())
                if as_exc:
                    obj = _load_exception(obj)
                return obj
            except Exception as error:
                leaked_proxy_ids = set(msg.get_uint() for _ in range(msg.get_uint())) - set(proxy_ids)
                if leaked_proxy_ids:
                    msg = _Message()
                    msg.put_uint(_DEL_OBJS)
                    msg.put_uint(len(leaked_proxy_ids))
                    for proxy_id in leaked_proxy_ids:
                        msg.put_uint(proxy_id)
                    self.__sender.send_frame(msg.as_bytes())
                raise OperationError('Unpickling failed failed due to {}'.format(
                    format_exception_only(error))) from error

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

        self.__executor.close()

        with self.__lock:
            self.__logger.info('Connection stopped')
            self.__state = _STOPPED
            self.__state_condition.notify_all()

        if self.__on_stopped is not None:
            self.__on_stopped(self)

    def __receive_superblock(self):
        while True:
            superblock = self.__framer.recv_superblock(_SUPERBLOCK_SIZE)
            magic, major, minor, patch = struct.unpack_from('>IBBB', superblock)
            if magic == _MAGIC:
                self.__logger.debug('Remote across version is %s.%s.%s', major, minor, patch)
                if (major, minor) != _version[:2]:
                    raise ProtocolError('Local across {} is not compatible with remote {}.{}.{}'.format(
                        __version__, major, minor, patch
                    ))
                self.__sender.send_frame(_get_greeting_frame(self.__timeout_ms))
                break
            elif magic == _BIOS_MAGIC:
                self.__logger.debug('Bootrapping needed with version at least %s.%s.%s', major, minor, patch)
                if (major, minor, patch) > _version:
                    raise ProtocolError('At least across {}.{}.{} is needed to bootstrap connection'.format(
                        major, minor, patch
                    ))
                options = Options(
                    timeout=self.__timeout_ms / 1000,
                )
                payload = get_bootstrap_line(_start, options).encode('ascii')
                self.__sender.send_frame(payload)
                self.__sender.send_superblock(_get_superblock())
            else:
                raise ProtocolError('Invalid magic: 0x{:x}'.format(magic))

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
                    raise ProtocolError('Invalid message in {} state: {}'.format(handlers_name, msg_type))
                if handler(self, msg) is False:
                    break

    def __handle_greeting_locked(self, msg):
        timeout_ms = msg.get_uint()
        if timeout_ms:
            # idle messages should be sent after a half of timeout passes
            idle_timeout = timeout_ms / 2000.0
        else:
            idle_timeout = None

        self.__logger.info('Connection is running, idle_timeout=%r', idle_timeout)
        self.__sender.update_idle_timeout(idle_timeout)
        self.__handlers_locked = self.__ready_handlers_locked
        self.__state = _RUNNING
        self.__was_running = True
        self.__state_condition.notify_all()

    def __handle_apply_locked(self, msg):
        if self.__state != _RUNNING:
            return
        self.__executor.submit(self.__process_apply, msg)

    def __process_apply(self, msg):
        call_id = msg.get_uint()
        try:
            # Deserialize
            obj = self.__deserialize(msg)

            # Execute
            with _ConnScope(self):
                try:
                    func, args, kwargs = obj
                    try:
                        value = func(*args, **kwargs)
                        result_type = _RESULT_SUCCESS
                    # If we allow raising OperationError between processes, then the other side wouldn't be able to
                    # determine whether the call failed to execute, or ended up generating OperationError. This
                    # is especially problematic for DisconnectError, which indicates that the connection is
                    # no longer usable.
                    except OperationError as error:
                        raise RuntimeError('Cannot raise {} between processes'.format(
                            error.__class__.__name__)) from error
                except Exception as error:
                    value = error
                    result_type = _RESULT_ERROR

            # Serialize
            msg = _Message()
            msg.put_uint(_RESULT)
            msg.put_uint(call_id)
            msg.put_uint(result_type)
            self.__serialize(msg, value, as_exc=result_type == _RESULT_ERROR)
        except OperationError as error:
            # Handle serialization errors. First, try to serialize the whole error. If that fails, give up using
            # pickle and send only the message.
            try:
                msg = _Message()
                msg.put_uint(_RESULT)
                msg.put_uint(call_id)
                msg.put_uint(_RESULT_ERROR)
                self.__serialize(msg, error, as_exc=True)
            except OperationError:
                msg = _Message()
                msg.put_uint(_RESULT)
                msg.put_uint(call_id)
                msg.put_uint(_RESULT_OPERATION_ERROR)
                msg.put_bytes(str(error).encode('utf-8', errors='replace'))

        self.__sender.send_frame(msg.as_bytes())

    def __handle_result_locked(self, msg):
        call_id = msg.get_uint()
        future = self.__calls.pop(call_id, None)
        if future is None:
            raise ProtocolError('Call not found: {}'.format(call_id))
        future.set_result(msg)

    def __handle_del_objs_locked(self, msg):
        for _ in range(msg.get_uint()):
            del self.__objs[msg.get_uint()]

    def __handle_goodbye_locked(self, msg):
        self.__set_stopping_locked()
        return False

    def __handle_idle_locked(self, msg):
        self.__logger.debug('Received idle frame')

    __ready_handlers_locked = {
        _APPLY: __handle_apply_locked,
        _RESULT: __handle_result_locked,
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

    def call_ref(_self, _func, *args, **kwargs):
        return _self.call(_apply_ref, _func, args, kwargs)

    def replicate(self, obj):
        return self.call(ref, obj)

    def execute(_self, _source, **kwargs):
        return _self.call(_execute, _source, kwargs)


def _export(modules):
    finder = get_connection()._finder
    if finder is None:
        raise ValueError('Modules may be exported only over bootstrapped connections')
    finder.export(modules)


def _shutdown():
    while _unclosed_connections:
        conn_close = next(iter(_unclosed_connections)).close
        try:
            conn_close()
        except BaseException:
            ignore_exception_at(conn_close)


atexit.register(_shutdown)


# Start a remotely bootstrapped connection. 'args' is the value of 'ACROSS' variable set by BIOS scripts.
def _start(options, args):
    _logger.debug('Creating bootstrapped connection, options=%s, args=%s', options, args)
    if args[0] == 'stdio':
        conn = Connection.from_stdio(options=options)
    elif args[0] == 'socket':
        conn = Connection.from_socket(args[1], options=options)
    else:
        raise RuntimeError('Unrecognized channel type {!r}, args={!r}'.format(args[0], args))
    with conn:
        conn.wait()


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
    if not isinstance(obj, tuple):  # None or bare exception
        return obj
    exc = obj[0]
    exc.__context__ = _load_exception(obj[1])
    exc.__cause__ = _load_exception(obj[3])
    exc.__suppress_context__ = obj[2]
    exc.__traceback__ = _load_traceback(obj[4])
    return exc


def _dump_exception(exc, memo=None):
    if exc is None:
        return None
    if memo is None:
        memo = set()
    # It can happen that __context__ and __cause__ are the same, better not dump same exception multiple times.
    if id(exc) in memo:
        return exc
    memo.add(id(exc))
    return (
        exc,
        _dump_exception(exc.__context__, memo),
        exc.__suppress_context__,
        _dump_exception(exc.__cause__, memo),
        _dump_traceback(exc.__traceback__),
    )


def _get_process_name():
    return '{}:{}'.format(socket.gethostname(), os.getpid())


def _dump_traceback(tb):
    entries = []
    packed_tb = [(_get_process_name(), entries)]
    while tb is not None:
        frame = tb.tb_frame
        globs = frame.f_globals
        if tb.tb_next is None and _packed_tb_var in globs:
            packed_tb += globs[_packed_tb_var]
        else:
            code = frame.f_code
            filename, lineno = code.co_filename, tb.tb_lineno
            line = linecache.getline(filename, lineno, globs)
            entries.append((filename, lineno, code.co_name, line))
        tb = tb.tb_next
    return packed_tb


_packed_tb_var = '__across_packed_tb'
_code_tpl = compile('raise ValueError', '<traceback generator>', 'exec')


# This function creates a types.TracebackType object (which requires raising some exception, by the way)
# that is specifically crafted so that when 'traceback' module tries to format it, 'formatted_tb' string
# is printed as well.
def _generate_traceback_object(formatted_tb, packed_tb):
    co_name = '{}\n{}'.format(_code_tpl.co_name, formatted_tb.rstrip())
    try:
        replace = _code_tpl.replace  # since Python 3.8
    except AttributeError:
        # argument order is described in help(types.CodeType)
        code = types.CodeType(
            _code_tpl.co_argcount,
            _code_tpl.co_kwonlyargcount,
            _code_tpl.co_nlocals,
            _code_tpl.co_stacksize,
            _code_tpl.co_flags,
            _code_tpl.co_code,
            _code_tpl.co_consts,
            _code_tpl.co_names,
            _code_tpl.co_varnames,
            _code_tpl.co_filename,
            co_name,
            _code_tpl.co_firstlineno,
            _code_tpl.co_lnotab,
            _code_tpl.co_freevars,
            _code_tpl.co_cellvars,
        )
    else:
        code = replace(co_name=co_name)

    try:
        exec(code, {_packed_tb_var: packed_tb})
    except ValueError:
        return sys.exc_info()[2].tb_next


def _load_traceback(packed_tb):
    buf = []
    for procname, entries in packed_tb:
        buf.append('  [Returned from process "{}"]\n'.format(procname))
        for filename, lineno, name, line in entries:
            buf.append('  File "{}", line {}, in {}\n'.format(filename, lineno, name))
            if line:
                buf.append('    {}\n'.format(line.strip()))
    return _generate_traceback_object(''.join(buf), packed_tb)


class Proxy(object):
    def __init__(self, proxy_id):
        self._Proxy__conn = get_connection()
        self.__id = proxy_id

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
    '__contains__', '__delitem__', '__getitem__', '__len__', '__setitem__', '__call__', '__iter__', '__next__'])


def _get_methods(cls):
    return [
        attr
        for attr in dir(cls)
        if callable(getattr(cls, attr)) and (not attr.startswith('_') or attr in _safe_magic)
    ]


def _make_auto_proxy_type(methods):
    scope = {'Proxy': Proxy, '_apply_method': _apply_method, '_SELF': _SELF}
    source = ['class _AutoProxy(Proxy):\n']

    for meth in methods:
        source.append(
            '    def {0}(self, *args, **kwargs):\n'
            '        result = self._Proxy__conn.call(_apply_method, self, {0!r}, args, kwargs)\n'
            '        return self if result is _SELF else result\n'.format(meth)
        )

    if len(source) == 1:
        source.append('    pass\n')

    exec(''.join(source), scope)
    return scope['_AutoProxy']


class Reference(object):
    def __init__(self, obj):
        assert not isinstance(obj, (Proxy, Reference))
        self.__obj = obj

    def __reduce__(self):
        if _conn_tls.proxy_ids is None:
            raise RuntimeError("Only 'across.Connection' objects can pickle 'across.Reference' objects")
        methods = _get_methods(type(self.__obj))
        proxy_id = _conn_tls.conn._put_obj(self.__obj)
        _conn_tls.proxy_ids.append(proxy_id)
        return _make_proxy, (proxy_id, methods)


def ref(obj):
    return Reference(obj)


class _SELF:
    pass


def _apply_method(obj, name, args, kwargs):
    result = getattr(obj, name)(*args, **kwargs)
    if result is obj:
        return _SELF
    elif name == '__iter__':
        return ref(result)
    else:
        return result


def _get_obj(id):
    return get_connection()._get_obj(id)


def _apply_ref(func, args, kwargs):
    return ref(func(*args, **kwargs))


def _make_proxy(proxy_id, methods):
    methods = frozenset(methods)
    proxy_type = _proxy_types.get(methods)
    if proxy_type is None:
        proxy_type = _proxy_types[methods] = _make_auto_proxy_type(methods)
    _conn_tls.proxy_ids.append(proxy_id)
    return proxy_type(proxy_id)


def get_connection():
    conn = _conn_tls.conn
    if conn is None:
        raise ValueError('Current thread is not associated with any connection')
    return conn


# Export
set_debug_level = set_debug_level
