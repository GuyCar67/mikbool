"""Distributed MD5 search - client: searches parts using all CPU cores."""

import argparse
import hashlib
import json
import os
import socket
import struct
import threading


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


def candidate_string(n, padding, digits):
    return f"{n:0{digits}d}" if padding else str(n)


def worker(start, end, target, padding, digits, stop_event, result_box):
    md5 = hashlib.md5
    for n in range(start, end):
        if stop_event.is_set():
            return
        s = candidate_string(n, padding, digits)
        if md5(s.encode("ascii")).hexdigest() == target:
            result_box["value"] = s
            stop_event.set()
            return


def process_batch(start, end, target, padding, digits, num_threads, stop_event):
    result_box = {"value": None}
    total = end - start
    if total <= 0:
        return None

    per = (total + num_threads - 1) // num_threads
    threads = []
    for i in range(num_threads):
        w_start = start + i * per
        w_end = min(w_start + per, end)
        if w_start >= w_end:
            break
        t = threading.Thread(target=worker,
                             args=(w_start, w_end, target, padding, digits,
                                   stop_event, result_box))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    return result_box["value"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    args = parser.parse_args()

    cores = max(1, args.threads)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.host, args.port))
    print("ready to catch")

    stop_event = threading.Event()
    activated = False

    try:
        while not stop_event.is_set():
            send_msg(sock, {"type": "get_work"})
            msg = recv_msg(sock)
            if msg is None:
                break

            mtype = msg.get("type")
            if mtype == "work":
                start, end = msg["start"], msg["end"]
                target = msg["target"].lower()
                padding, digits = msg["padding"], msg["digits"]

                if not activated:
                    print("client activated")
                    activated = True

                answer = process_batch(start, end, target, padding, digits,
                                       cores, stop_event)
                if answer is not None:
                    print(f"found: {answer}")
                    send_msg(sock, {"type": "found", "value": answer})
                    recv_msg(sock)
                    break
                send_msg(sock, {"type": "done", "start": start, "end": end})

            elif mtype == "wait":
                threading.Event().wait(0.5)

            elif mtype == "stop":
                stop_event.set()
                break
    except (ConnectionError, OSError):
        pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()