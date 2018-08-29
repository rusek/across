API Reference
=============

Core functionality
------------------

.. module:: across

.. class:: Connection(channel)

    Connection objects enable easy interaction with remote Python processes by allowing you to execute
    arbitrary Python functions in remote processes, and vice versa.

    .. method:: call(func, /, *args, **kwargs)

        Call a given function remotely with supplied arguments, and pass back function return value / raised exception.

        All arguments provided to :meth:`call` method, as well as its return value / raised exception,
        must be pickleable.

        Apart from an exception raised by a given function, this method may also raise :exc:`OperationError`
        to indicate an internal error (pickling / unpickling error, lost connection with a remote process,
        etc.).

    .. method:: create(func, /, *args, **kwargs)

        Similar to :meth:`call`, but instead of passing back function return value directly, return a proxy
        referencing it. See :class:`Proxy` for more information.

        :meth:`create` is useful when a given function returns an unpickleable object:

        .. code-block:: python

            fh = conn.call(open, 'extinct_species', 'w')
            fh.write('dinosaurs\n')
            fh.write('unicorns\n')
            fh.close()

    .. method:: replicate(obj)

        Create a remote copy of a given object, and return a proxy referencing it.

        For :meth:`replicate` method to work, *obj* must be pickleable. This method may rairse :exc:`OperationError`
        to indicate an internal error (pickling / unpickling error, lost connection with a remote process, etc.).

    .. method:: close()

        Close the connection to a remote process, and free all resources allocated by the connection.

        If, between opening and closing the connection, a *fatal error* occurred, then this method raises an
        exception (e.g. :class:`ProtocolError`).

    .. method:: wait()

        Wait for remote process to close the connection.

.. class:: Proxy

    Proxies act as references to objects living in remote Python processes. All method calls on a proxy are
    forwarded to a remote object, and executed there.

    Example:

    .. code-block:: python

        cheeses = conn.create(list)
        cheeses.append('brie')
        cheeses.append('gouda')

    Proxies can also be passed as arguments to remote functions:

    .. code-block:: python

        def append_to(col, item):
            col.append(item)

        cheeses = conn.create(list)
        append_to(cheeses, 'cheddar')

    It is also possible to retrieve a copy of a referenced object:

    .. code-block:: python

        import copy

        local_cheeses = copy.deepcopy(cheeses)

    Not every method is forwarded by proxies. Following methods are supported:

    * public methods, that is, those which name doesn't start with an underscore (``_``);
    * container special methods: ``__len__``, ``__contains__``, ``__getitem__``, ``__setitem__``, ``__delitem__``;
    * ``__call__``.

.. class:: Local

    Marker for an object indicating that wrapped object should be passed to / returned from a remote process
    as a proxy.

    .. code-block:: python

        def create_list():
            return Local([])

        remote_cheeses = conn.call(create_list)
        remote_cheeses.append('gorgonzola')

    .. code-block:: python

        def append_feta(cheeses):
            cheeses.append('feta')

        local_cheeses = []
        conn.call(append_feta, Local(local_cheeses))


.. function:: get_connection()

    Return a connection associated with the current thread.

.. exception:: OperationError

    :exc:`OperationError`, and its subclass -- :exc:`DisconnectError` --  are raised by :class:`Connection`
    methods to indicate an internal operation error. Internal connection errors fall into two categories:

    -   *non-fatal errors* cause only a single operation (e.g. a single invocation of :meth:`Connection.call` method)
        to fail, and are signalled by raising :exc:`OperationError` directly; an example of a non-fatal error
        is a pickling / unpickling error;

    -   *fatal errors* break the whole connection, meaning that all successive operations on that connection
        will fail; fatal errors are signalled by raising :exc:`DisconnectError`; an example of a fatal error
        is an I/O error while communicating with a remote process.

.. exception:: DisconnectError

    Subclass of :exc:`OperationError`, raised in case of fatal connection errors.

.. exception:: ProtocolError

    Exception indicating that an internal protocol error occurred while communicating with a remote process.

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
