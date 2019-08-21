Run Python code across the universe
===================================

.. image:: https://travis-ci.org/rusek/across.svg?branch=master
    :target: https://travis-ci.org/rusek/across

.. image:: https://coveralls.io/repos/github/rusek/across/badge.svg?branch=master
    :target: https://coveralls.io/github/rusek/across?branch=master

.. image:: https://readthedocs.org/projects/acrosspy/badge/?version=latest
    :target: https://acrosspy.readthedocs.io/en/latest/?badge=latest

.. image:: https://img.shields.io/pypi/v/across
    :target: https://pypi.org/project/across/

.. image:: https://img.shields.io/pypi/l/across

`across` is an RPC library with the primary focus on running arbitrary Python code on remote machines without
boilerplate. It can also seamlessly transfer module sources to remote machines so you don't have to upload any files
by yourself.

Example of using `across` over SSH, with nothing but a Python interpreter installed on a remote host:

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

Highlights:

- ``across`` works with any object/function that can be serialized with Python built-in pickle module.
- You can expose local modules for importing them remotely without the need to upload any files.
- Exceptions are passed back to the caller along with a complete traceback indicating exception origin, just
  as you would expect.
- A single connection may be used for executing code from different threads in parallel.
- Proxies allow you to create references to remotely living objects.
- Connections are symmetrical -- there's nothing stopping you from running a local function from a remote host
  if needed.
- ``across`` constantly monitors connection state with heartbeat messages, enabled by default.

Useful links:

- Documentation: https://acrosspy.readthedocs.io/
- Source code: https://github.com/rusek/across
- Issue tracker: https://github.com/rusek/across/issues
