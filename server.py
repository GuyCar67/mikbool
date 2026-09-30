"""Distributed MD5 search - server: hands out parts and collects the answer."""

import argparse
import json
import os
import socket
import struct
import threading
import time
from collections import deque


def send_msg(sock, obj):
    data = json.dumps(obj).encode("utf-8")
    sock.sendall(struct.pack("!I", len(data)) + data)


def _recv_exactly(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def recv_msg(sock):
    header = _recv_exactly(sock, 4)
    if header is None:
        return None
    (length,) = struct.unpack("!I", header)
    body = _recv_exactly(sock, length)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


TARGET_MD5 = os.environ.get("MD5_TARGET", "ec9c0f7edcc18a98b1f31853b1813301")
TOTAL = int(os.environ.get("MD5_TOTAL", "10000000000"))      # search [0, TOTAL)
DIGITS = int(os.environ.get("MD5_DIGITS", "10"))             # only used if PADDING
PADDING = os.environ.get("MD5_PADDING", "0") in ("1", "true", "True")
CHUNK_SIZE = int(os.environ.get("MD5_CHUNK", "50000"))       # numbers per part
LEASE_SECONDS = float(os.environ.get("MD5_LEASE", "60"))     # dead-client backstop


class WorkManager:
    def __init__(self, total, chunk_size, lease_seconds):
        self.total = total
        self.chunk_size = chunk_size
        self.lease_seconds = lease_seconds

        self.cursor = 0
        self.pool = deque()      # parts returned by dead clients, retried first
        self.assigned = {}       # (start, end) -> {"client": id, "expires": ts}
        self.done = []
        self.covered = 0

        self.found_value = None
        self.finished = False
        self.lock = threading.Lock()

    def _reap_expired(self):
        now = time.time()
        for rng, info in list(self.assigned.items()):
            if info["expires"] < now:
                del self.assigned[rng]
                self.pool.append(rng)

    def _carve_fresh(self):
        if self.cursor >= self.total:
            return None
        start = self.cursor
        end = min(self.cursor + self.chunk_size, self.total)
        self.cursor = end
        return (start, end)

    def _space_confirmed_done(self):
        return self.cursor >= self.total and not self.pool and not self.assigned

    def request_work(self, client_id):
        with self.lock:
            if self.finished:
                return ("stop", self.found_value)
            self._reap_expired()
            rng = self.pool.popleft() if self.pool else self._carve_fresh()
            if rng is None:
                # Nothing fresh: done only if every part came back, else wait for
                # parts still out with other clients (they may yet be requeued).
                if self._space_confirmed_done():
                    self.finished = True
                    return ("stop", None)
                return ("wait", None)
            self.assigned[rng] = {"client": client_id,
                                  "expires": time.time() + self.lease_seconds}
            return ("work", rng)

    def complete(self, client_id, rng):
        rng = (rng[0], rng[1])
        with self.lock:
            if rng in self.assigned:
                del self.assigned[rng]
                self.done.append(rng)
                self.covered += rng[1] - rng[0]
            if self._space_confirmed_done() and not self.finished:
                self.finished = True

    def report_found(self, value):
        with self.lock:
            if self.found_value is None:
                self.found_value = value
            self.finished = True

    def client_gone(self, client_id):
        # Return every part this client held so it gets searched by someone else.
        with self.lock:
            for rng, info in list(self.assigned.items()):
                if info["client"] == client_id:
                    del self.assigned[rng]
                    self.pool.append(rng)

    def is_finished(self):
        with self.lock:
            return self.finished


def handle_client(conn, addr, client_id, manager):
    try:
        while True:
            msg = recv_msg(conn)
            if msg is None:
                break

            mtype = msg.get("type")
            if mtype == "get_work":
                kind, payload = manager.request_work(client_id)
                if kind == "work":
                    start, end = payload
                    send_msg(conn, {"type": "work", "start": start, "end": end,
                                    "target": TARGET_MD5.lower(),
                                    "padding": PADDING, "digits": DIGITS})
                elif kind == "wait":
                    send_msg(conn, {"type": "wait"})
                else:
                    send_msg(conn, {"type": "stop", "value": payload})
                    break
            elif mtype == "done":
                manager.complete(client_id, (msg["start"], msg["end"]))
            elif mtype == "found":
                manager.report_found(msg.get("value"))
                send_msg(conn, {"type": "stop", "value": msg.get("value")})
                break
    except (ConnectionError, OSError):
        pass
    finally:
        conn.close()
        manager.client_gone(client_id)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    manager = WorkManager(TOTAL, CHUNK_SIZE, LEASE_SECONDS)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(64)
    print(f"[server] listening on {args.host}:{args.port}, target {TARGET_MD5}")

    next_id = 0
    try:
        while not manager.is_finished():
            srv.settimeout(1.0)          # so we notice 'finished' between accepts
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            conn.settimeout(None)
            next_id += 1
            threading.Thread(target=handle_client,
                             args=(conn, addr, next_id, manager),
                             daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()

    if manager.found_value is not None:
        print(f"[server] found: {manager.found_value}")
    else:
        print("[server] not found")


if __name__ == "__main__":
    main()