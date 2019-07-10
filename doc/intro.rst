Introduction
============

Two-minute tutorial
-------------------

Below example shows how to run Python code remotely, via ``ssh``:

.. code-block:: python

    import shlex
    import socket
    import across

    def get_greeting():
        return 'Hello from {}!'.format(socket.gethostname())

    def main():
        cmd = 'python3 -c ' + shlex.quote(across.get_bios())
        with across.Connection.from_command(['ssh', 'wonderland', cmd]) as conn:
            conn.export('__main__')
            print(conn.call(get_greeting))

    if __name__ == '__main__':
        main()

Now, let's go through all the different pieces, one by one.

.. code-block:: python

    import shlex
    import socket
    import across

Some imports.

.. code-block:: python

    def get_greeting():
        return 'Hello from {}!'.format(socket.gethostname())

This is the function we would like to run remotely. You can do whatever you like inside.

.. code-block:: python

        def main():

``main`` function begins.

.. code-block:: python

        cmd = 'python3 -c ' + shlex.quote(across.get_bios())

That's the shell script we are going to run through ``ssh``. It basically starts a Python interpreter
running some ``across`` code. Thanks to ``across`` magic, installing ``across`` on a remote server
is not necessary!

.. code-block:: python

        with across.Connection.from_command(['ssh', 'wonderland', cmd]) as conn:

Here we start ``ssh`` process that connects to ``wonderland`` server.

.. code-block:: python

            conn.export('__main__')

We need to tell ``across`` that the local ``__main__`` module should be accessible remotely, otherwise
``get_greeting()`` function will not be available on a remote site. ``across`` will not guess that for us.
Explicit is better than implicit.

.. code-block:: python

            print(conn.call(get_greeting))

Functions invoked via :meth:`Connection.call <across.Connection.call>` method are executed remotely,
so ``get_greeting()`` function will be run on ``wonderland`` server, and the result will be passed back
to the current process, where it will get printed.

That's where ``main()`` ends.

.. code-block:: python

    if __name__ == '__main__':
        main()

Standard script guard. If you use ``conn.export('__main__')``, then you need to include it, otherwise
your script would run remotely. Fortunately, ``across`` detects missing guard, and raises exception.
