"""A 60-line Redis client (RESP protocol) so ShopStream needs no pip install.

redis-py works the same way; this one is small enough to read in one sitting.
"""
import os
import socket
import threading

HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
PORT = int(os.environ.get("REDIS_PORT", "6390"))


class Redis:
    def __init__(self, host=HOST, port=PORT):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.buf = self.sock.makefile("rb")

    def call(self, *args):
        out = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.sock.sendall(b"".join(out))
        return self._read()

    def _read(self):
        line = self.buf.readline()
        kind, rest = line[:1], line[1:-2]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n == -1:
                return None
            data = self.buf.read(n + 2)[:-2]
            return data.decode()
        if kind == b"*":
            n = int(rest)
            return None if n == -1 else [self._read() for _ in range(n)]
        raise RuntimeError(f"bad reply {line!r}")


_local = threading.local()


def client():
    """One connection per thread."""
    if not hasattr(_local, "r"):
        _local.r = Redis()
    return _local.r
