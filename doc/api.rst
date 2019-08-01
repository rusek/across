API Reference
=============

Core functionality
------------------

.. module:: across

.. class:: Connection

    Connection lets you run Python code in a remote Python process, and vice versa.

    To create a connection, use one of ``from_*`` methods documented below. Be aware that ``from_*`` methods
    typically establish connection in background. If you want to make sure that connection handshake was successful,
    you should use ``with`` statement:

    .. code-block:: python

        # __enter__ waits for handshake to complete
        # __exit__ takes care of calling conn.close()
        with Connection.from_tcp('wonderland', 1865) as conn:
            conn.call(os.mkdir, '/home/alice')

    However, waiting for handshake to complete is optional, so the above can be rewritten as:

    .. code-block:: python

        conn = Connection.from_tcp('wonderland', 1865)
        try:
            conn.call(os.mkdir, '/home/alice')
        finally:
            conn.close()

    Connections are multithreaded.

    .. classmethod:: from_tcp(host, port, *, options=None)

        Connect to a remote host over TCP.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. classmethod:: from_unix(path, *, options=None)

        Connect over Unix domain sockets.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. classmethod:: from_socket(sock, *, options=None)

        Wrap an already connected socket in a :class:`Connection` object.

        This method may be used by server implementations to handle accepted sockets.

        ``sock`` should be a socket created with :func:`socket.socket` function.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. classmethod:: from_pipes(in_pipe, out_pipe, *, options=None)

        Wrap a pair of input/output pipes in a :class:`Connection` object.

        ``in_pipe`` and ``out_pipe`` should be file objects obtained e.g. by passing :func:`os.pipe`
        descriptors to :func:`os.fdopen` function.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. classmethod:: from_stdio(*, options=None)

        Create a connection over process standard input/output pipes.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. classmethod:: from_command(args, *, options=None)

        Spawn a child process with a given list of command line arguments (first being an executable path) and
        communicate with it over standard input/output pipes.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. classmethod:: from_shell(script, *, options=None)

        Spawn a shell process executing a given shell script, and communicate with it over standard input/output pipes.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. classmethod:: from_process(proc, *, options=None)

        Wrap an existing child process in a :class:`Connection` object.

        ``proc`` should be an instance of :class:`subprocess.Popen` class.

        ``options``, if specified, should be an instance of :class:`Options` class.

    .. method:: export(*modules)

        Make one or more local modules importable remotely.

        Only top-level modules may be exported. When a module is exported, all its submodules are considered exported
        as well.

        This method may be used to make functions/classes defined in a local ``__main__`` module accessible
        remotely.

        When importing a module remotely, local exported modules take precedence over modules installed remotely.

        For this method to work, the connection on the other side must be created with
        :attr:`accept_exported_modules <Options.accept_exported_modules>` option set to :obj:`True`. This is a
        safety precaution to prevent a connection from injecting its local modules into a remote process that
        shared among many connections.

    .. method:: call(func, /, *args, **kwargs)

        Call a given function remotely with supplied arguments, and pass back function return value / raised exception.

        All arguments provided to :meth:`call` method, as well as its return value / raised exception,
        must be pickleable.

        Apart from an exception raised by a given function, this method may also raise :exc:`OperationError`
        to indicate an internal error (pickling / unpickling error, lost connection with a remote process,
        etc.).

    .. method:: call_ref(func, /, *args, **kwargs)

        Similar to :meth:`call`, but instead of passing back function return value directly, return a proxy
        referencing it. See :class:`Proxy` for more information.

        :meth:`call_ref` is useful when a given function returns an unpickleable object:

        .. code-block:: python

            fh = conn.call(open, 'extinct_species', 'w')
            fh.write('dinosaurs\n')
            fh.write('unicorns\n')
            fh.close()

    .. method:: replicate(obj)

        Create a remote copy of a given object, and return a proxy referencing it.

        For :meth:`replicate` method to work, ``obj`` must be pickleable. This method may rairse :exc:`OperationError`
        to indicate an internal error (pickling / unpickling error, lost connection with a remote process, etc.).

    .. method:: execute(source, /, **vars)

        Execute a snippet of Python code remotely, and return result of the last expression.

        .. code-block:: python

            conn.execute('2 + 2')
            conn.execute('two = 2; two + two')
            conn.execute('x + x', x=2)

    .. method:: cancel()

        Cancel all operations and forcibly disconnect from a remote Python process.

        This method may be called at any time, and can be used to abort e.g. ``__enter__`` or :meth:`close` methods
        being called in a different thread. :meth:`close` after :meth:`cancel` will still take care of freeing all the
        resources, but clean protocol shutdown will be skipped.

    .. method:: close()

        Close the connection to a remote process, and free all resources allocated by the connection.

        If, between opening and closing the connection, a *fatal error* occurred, then this method raises an
        exception (e.g. :class:`ProtocolError`). Exception will be raised also after a remote process died
        during communication, or :meth:`cancel` was called on a remote connection.

    .. method:: wait()

        Wait for remote process to close the connection.

.. function:: get_bios()

    Get a Python script that, when executed via ``python -c``, creates a connection over standard input/output pipes.
    This script has no library dependencies, so it can be remotely executed (e.g. via ``ssh``) on machines where
    `across` is not installed:

    .. code-block:: python

        from shlex import quote

        cmd = 'python3 -c ' + quote(get_bios())
        with Connection.from_command(['ssh', 'wonderland', cmd]):
            conn.call(os.mkdir, '/home/alice')

    A remote connection object constructed this way has :attr:`accept_exported_modules
    <Options.accept_exported_modules>` option set to :obj:`True`.

.. class:: Options(**options)

    With options you can tweak connections for your specific use case.

    .. attribute:: timeout

        Timeout for communication operations: sending/receiving data, connecting to a remote host, etc. The default
        value tries to keep a balance between detecting broken connections fast and avoiding false positives.
        Currently, it's one minute.

    .. attribute:: accept_exported_modules

        Boolean value indicating whether modules from a remote connection site can be imported into the current
        process (see :meth:`Connection.export`). Defaults to :obj:`False`.

    .. method:: copy(**options)

        Create a copy of the current options, optionally overriding some values.

.. class:: Proxy

    Proxies act as handles to objects living in remote Python processes. All method calls on a proxy are
    forwarded to a remote object, and executed there.

    Example:

    .. code-block:: python

        cheeses = conn.call_ref(list)
        cheeses.append('brie')
        cheeses.append('gouda')

    Proxies can also be passed as arguments to remote functions:

    .. code-block:: python

        def append_to(col, item):
            col.append(item)

        cheeses = conn.call_ref(list)
        append_to(cheeses, 'cheddar')

    It is also possible to retrieve a copy of a referenced object:

    .. code-block:: python

        import copy

        local_cheeses = copy.deepcopy(cheeses)

    Not every method is forwarded by proxies. Following methods are supported:

    * public methods, that is, those which name doesn't start with an underscore (``_``);
    * container special methods: ``__len__``, ``__contains__``, ``__getitem__``, ``__setitem__``, ``__delitem__``;
    * ``__call__``.

    To retrieve connection associated with a proxy, use :func:`get_proxy_connection` function.

.. function:: get_proxy_connection(proxy)

    Return a connection object associated with a given proxy.

.. function:: ref(obj)

    Wrap a given object in a special marker indicating that the object should be passed / returned to a remote process
    as a proxy.

    .. code-block:: python

        def create_list():
            return ref([])

        remote_cheeses = conn.call(create_list)
        remote_cheeses.append('gorgonzola')

    .. code-block:: python

        def append_feta(cheeses):
            cheeses.append('feta')

        local_cheeses = []
        conn.call(append_feta, ref(local_cheeses))


.. class:: Reference

    Type of objects returned by :func:`ref` function.

.. function:: get_connection()

    Return a connection associated with the current thread.

.. exception:: OperationError

    :exc:`OperationError`, and its subclass -- :exc:`DisconnectError` --  are raised by :class:`Connection`
    methods to indicate an internal operation error. Internal connection errors fall into two categories:

    -   *non-fatal errors* cause only a single operation (e.g. a single invocation of :meth:`Connection.call` method)
        to fail, and are signalled by raising :exc:`OperationError` directly; an example of a non-fatal error
        is a pickling / unpickling error;

    -   *fatal errors* break the whole connection, meaning that all successive operations on that connection
        will fail as well; fatal errors are signalled by raising :exc:`DisconnectError`; an example of a fatal error
        is an I/O error while communicating with a remote process.

.. exception:: DisconnectError

    Subclass of :exc:`OperationError`, raised in case of fatal connection errors.

.. exception:: ProtocolError

    Exception indicating that an internal protocol error occurred while communicating with a remote process.

.. function:: set_debug_level(level)

    Print logs to standard error output. Following values of ``level`` are recognized:

    -   ``0`` -- no output (default setting);
    -   ``1`` -- print INFO, WARNING and ERROR messages;
    -   ``2`` -- print additionally DEBUG messages.

Logging integration
-------------------

.. module:: across.logging

.. class:: AcrossHandler(conn=None)

    Create a log handler suitable for use with :mod:`logging` module. All messages logged through this handler
    will be forwarded to the corresponding logger in the remote process referenced by a given connection.

    Example:

    .. code-block:: python

        import logging
        import across
        import across.logging

        logger = logging.getLogger('rainbow')

        def remote_main():
            # set up logging
            logger.setLevel(logging.DEBUG)
            handler = across.logging.AcrossHandler()
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)

            # do some stuff here
            logger.info('Doing some stuff')

        def main():
            # set up logging
            logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)

            with across.Connection(...) as conn:
                conn.call(remote_main)

Servers
-------

.. module:: across.servers

.. class:: ConnectionHandler

    Abstract base class for connection handlers. Connection handlers are responsible for wrapping accepted
    sockets in :class:`.Connection` objects, and managing a pool of running connections.

    Example:

    .. code-block:: python

        with LocalConnectionHandler() as handler:
            sock = socket.socket()
            sock.bind(('wonderland', 1865))
            sock.listen(socket.SOMAXCONN)
            while True:
                handler.handle_socket(sock.accept()[0])

    .. method:: handle_socket(sock)

        Create :class:`.Connection` object around a given socket.

        ``sock`` should be a socket object created with :func:`socket.socket` function.

    .. method:: cancel()

        Cancel all running connections.

        This method is non-blocking. :meth:`close` must be called to properly free all the resources.

    .. method:: close()

        Close all running connections. Unless :meth:`cancel` has been called before, connections are
        closed gracefully.

.. class:: LocalConnectionHandler(*, options=None)

    Subclass of :class:`ConnectionHandler` that creates connections in the current process.

    ``options``, if specified, are passed to created connections. The value of ``options`` should be an instance
    of :class:`.Options` class.

.. class:: ProcessConnectionHandler(*, options=None)

    Subclass of :class:`ConnectionHandler` that creates a child process for each accepted socket.

    ``options``, if specified, are passed to created connections. The value of ``options`` should be an instance
    of :class:`.Options` class.

.. class:: BIOSConnectionHandler(*, options=None)

    Similar to :class:`ProcessConnectionHandler`, but created child processes do not import a local version of
    `across` library. Instead, `across` sources are fetched from a remote peer.

    ``options``, if specified, should be an instance of :class:`.Options` class. :attr:`.Options.timeout`
    specifies how long :meth:`close() <ConnectionHandler.close>` will wait for child processes to terminate gracefully before killing them.
    Other options are ignored.

.. function:: run_tcp(host, port, *, handler=None)

    Accept connections on a given TCP endpoint until :exc:`KeyboardInterrupt` is received.

    ``handler``, if provided, should be an instance of :class:`ConnectionHandler` class.
    Defaults to :class:`LocalConnectionHandler`.

.. function:: run_unix(path, *, handler=None)

    Create Unix domain socket and accept connections until :exc:`KeyboardInterrupt` is received.

    ``handler``, if provided, should be an instance of :class:`ConnectionHandler` class.
    Defaults to :class:`LocalConnectionHandler`.
