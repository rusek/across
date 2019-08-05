Cookbook
========

Aborting long-running operations
--------------------------------

For long-running operations, periodically call :meth:`Connection.is_connected <across.Connection.is_connected>`
to check if connection is still alive.

.. code-block:: python

    import across

    def copy_file(src, dest):
        with open(src, 'rb') as src_handle:
            with open(dest, 'wb') as dest_handle:
                while True:
                    data = src_handle.read(64 * 1024)
                    if not data or not across.get_connection().is_connected():
                        break
                    dest_handle.write(data)

    def main():
        with across.Connection.from_tcp('wonderland', 1865) as conn:
            conn.export('__main__')
            conn.call(copy_file, 'data', 'data.bak')

    if __name__ == '__main__':
        main()
