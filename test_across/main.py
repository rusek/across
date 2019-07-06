import across

from .utils import par, mktemp, localhost, skip_if_no_unix_sockets

import unittest
import subprocess
import sys
import socket
import os
import time
import signal


def spawn_main(args, **popen_extras):
    return subprocess.Popen(
        [sys.executable, '-m', 'across'] + args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        **popen_extras
    )


def run_main(args):
    process = spawn_main(args)
    process.stdin.close()
    stdout = process.stdout.read()
    process.stdout.close()
    rc = process.wait()
    return rc, stdout.decode()


def serve(sock):
    with across.Connection.from_socket(sock.accept()[0]) as conn:
        conn.wait()


class MainTest(unittest.TestCase):
    def test_version(self):
        rc, out = run_main(['--version'])
        self.assertEqual(rc, 0)
        self.assertEqual(out.strip(), across.__version__)

    def test_tcp_client(self):
        sock = socket.socket()
        sock.bind((localhost, 0))
        sock.listen(1)
        (rc, out), _ = par(
            lambda: run_main([
                '--tcp',
                '{}:{}'.format(localhost, sock.getsockname()[1]),
                '--execute',
                'import os; os.getpid()',
            ]),
            lambda: serve(sock),
        )
        sock.close()
        self.assertEqual(rc, 0)
        self.assertEqual(int(out.strip()), os.getpid())

    def test_bad_usage(self):
        args_list = [
            # no action/address
            [],
            # no action
            ['--stdio'],
            # no address
            ['--wait'],
            # stdio in server mode
            ['--server', '--stdio'],
            # action in server mode
            ['--server', '--tcp', '{}:0'.format(localhost), '--execute', 'pass'],
            # bad TCP addresses
            ['--server', '--tcp', ''],
            ['--server', '--tcp', 'bad'],
            ['--server', '--tcp', '{}:bad'.format(localhost)],
        ]
        for args in args_list:
            process = spawn_main(args, stderr=subprocess.PIPE)
            out, err = process.communicate()
            self.assertEqual(process.wait(), 2)
            self.assertEqual(out, b'')


@skip_if_no_unix_sockets
class UnixMainTest(unittest.TestCase):
    def test_unix_client(self):
        path = mktemp()
        sock = socket.socket(socket.AF_UNIX)
        sock.bind(path)
        sock.listen(1)
        (rc, out), _ = par(
            lambda: run_main([
                '--unix',
                path,
                '--execute',
                'import os; os.getpid()',
            ]),
            lambda: serve(sock),
        )
        sock.close()
        self.assertEqual(rc, 0)
        self.assertEqual(int(out.strip()), os.getpid())

    def test_unix_server(self):
        path = mktemp()
        process = spawn_main(['--server', '--unix', path])
        sock = socket.socket(socket.AF_UNIX)
        while sock.connect_ex(path) != 0:
            time.sleep(0.01)
            self.assertIsNone(process.poll())
        os.kill(process.pid, signal.SIGINT)
        process.wait()
        sock.close()
        process.stdin.close()
        process.stdout.close()
