from . import Connection
from .channels import PipeChannel
import sys

channel = PipeChannel(sys.stdin.buffer, sys.stdout.buffer)
with Connection(channel) as conn:
    conn.wait()
