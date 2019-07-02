import argparse

from . import __version__, Connection
from .servers import run_tcp, run_unix, BootstrappingConnectionHandler


def _parse_args():
    parser = argparse.ArgumentParser(
        description='Establish across connections.',
    )
    parser.add_argument(
        '--version',
        action='version',
        version=__version__,
    )
    parser.add_argument(
        '--server',
        action='store_true',
        help='switch to server mode: listen for incoming connections',
    )
    parser.add_argument(
        '--tcp',
        metavar='HOST:PORT',
        help='connect over TCP',
        type=_parse_tcp,
        dest='address',
    )
    parser.add_argument(
        '--unix',
        metavar='PATH',
        help='connect over Unix domain socket',
        type=lambda arg: ('unix', arg),
        dest='address',
    )
    parser.add_argument(
        '--stdio',
        help='connect over stdin/stdout pipes',
        action='store_const',
        const=('stdio',),
        dest='address',
    )
    parser.add_argument(
        '--execute',
        metavar='COMMAND',
        help='execute Python code remotely',
        type=lambda arg: ('execute', arg),
        dest='action',
    )
    parser.add_argument(
        '--wait',
        help='block until connection is remotely closed',
        action='store_const',
        const=('wait',),
        dest='action'
    )

    args = parser.parse_args()
    if not args.address:
        parser.error('address has not been specified')
    if args.server and args.action:
        parser.error('actions are not supported in server mode')
    if not args.server and not args.action:
        parser.error('action has not been specified')
    if args.server and args.address[0] == 'stdio':
        parser.error('cannot start server over stdin/stdout pipes')

    return args


def _parse_tcp(arg):
    try:
        if arg.count(':') != 1:
            raise ValueError
        host, port = arg.rsplit(':', 1)
        return 'tcp', host, int(port)
    except ValueError:
        raise argparse.ArgumentTypeError('invalid value: {!r}'.format(arg))


def _handle_args(args):
    if args.server:
        _run_server(args)
    else:
        _run_client(args)


def _run_server(args):
    handler = BootstrappingConnectionHandler()
    if args.address[0] == 'tcp':
        run_tcp(args.address[1], args.address[2], handler=handler)
    elif args.address[0] == 'unix':
        run_unix(args.address[1], handler=handler)
    else:
        raise AssertionError(args.address)


def _run_client(args):
    if args.address[0] == 'tcp':
        conn = Connection.from_tcp(args.address[1], args.address[2])
    elif args.address[0] == 'unix':
        conn = Connection.from_unix(args.address[1])
    elif args.address[0] == 'stdio':
        conn = Connection.from_stdio()
    else:
        raise AssertionError(args.address)
    with conn:
        if args.action[0] == 'wait':
            conn.wait()
        elif args.action[0] == 'execute':
            obj = conn.execute(args.action[1])
            if obj is not None:
                print(obj)
        else:
            raise AssertionError(args.action)


def main():
    _handle_args(_parse_args())


if __name__ == '__main__':
    main()
