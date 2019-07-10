import unittest
import across
import across.channels
import operator
import sys
import subprocess

from .utils import windows

if windows:
    def shell_quote(text):
        # cmd.exe quoting is totally ridiculous, let's just handle basic case.
        if '"' in text or text.endswith('\\'):
            raise NotImplementedError('Quoting not implemented for {!r}'.format(text))
        return '"{}"'.format(text)
else:
    from shlex import quote as shell_quote


base_args = [sys.executable, '-m', 'across', '--stdio', '--wait']


class ProcessTestCase(unittest.TestCase):
    def test_from_command(self):
        with across.Connection.from_command(base_args) as conn:
            self.assertEqual(conn.call(operator.add, 2, 2), 4)

    def test_from_command_no_action(self):
        with across.Connection.from_command(base_args):
            pass

    def test_from_shell(self):
        with across.Connection.from_shell(' '.join(map(shell_quote, base_args))) as conn:
            self.assertEqual(conn.call(operator.add, 2, 2), 4)

    def test_from_process(self):
        proc = subprocess.Popen(base_args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        with across.Connection.from_process(proc) as conn:
            self.assertEqual(conn.call(operator.add, 2, 2), 4)

    def test_from_process_no_stdin_pipe(self):
        proc = subprocess.Popen([sys.executable, '-c', ''], stdout=subprocess.PIPE)
        self.assertRaises(ValueError, across.Connection.from_process, proc)
        proc.stdout.close()
        proc.wait()

    def test_from_process_no_stdout_pipe(self):
        proc = subprocess.Popen([sys.executable, '-c', ''], stdin=subprocess.PIPE)
        self.assertRaises(ValueError, across.Connection.from_process, proc)
        proc.stdin.close()
        proc.wait()
