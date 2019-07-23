import unittest
import subprocess
import sys
import socket
import os
import time
import signal
import argparse

import across
from across.__main__ import _parse_tcp

from .utils import par, mktemp, localhost, localhost_ipv6, anyaddr, skip_if_no_unix_sockets


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


class ParserTest(unittest.TestCase):
    def test_parse_tcp(self):
        for text, address in [
            ('wonderland:1865', ('wonderland', 1865)),
            ('wonderland:0', ('wonderland', 0)),
            ('127.0.0.1:65535', ('127.0.0.1', 65535)),
            ('[::1]:20', ('::1', 20)),
            ('[::ffff:192.0.2.128]:20', ('::ffff:192.0.2.128', 20)),
        ]:
            self.assertEqual(_parse_tcp(text), ('tcp',) + address)

        for text in [
            'bad',
            '20',
            'bad:-20',
            'bad:100000',
            '[bad]:20',
            '[bad.bad]:20',
            '::1:20',
            '[[::1]:20',
            '[]::1]:20',
            'ba[d:20',
            'ba]d:20',
        ]:
            self.assertRaises(argparse.ArgumentTypeError, _parse_tcp, text)


# Note that tests using this function have a race condition :(
def _random_port():
    sock = socket.socket()
    sock.bind((anyaddr, 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


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

    def test_tcp_server(self):
        self._run_tcp_server_test(socket.AF_INET)

    def test_tcp_server_ipv6(self):
        self._run_tcp_server_test(socket.AF_INET6)

    def _run_tcp_server_test(self, family):
        if family == socket.AF_INET6:
            host = localhost_ipv6
            escaped_host = '[{}]'.format(host)
        else:
            host = escaped_host = localhost
        port = _random_port()
        process = spawn_main(['--server', '--tcp', '{}:{}'.format(escaped_host, port)])
        sock = socket.socket(family)
        while sock.connect_ex((host, port)) != 0:
            time.sleep(0.01)
            self.assertIsNone(process.poll())
        os.kill(process.pid, signal.SIGINT)
        process.wait()
        sock.close()
        process.stdin.close()
        process.stdout.close()

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
