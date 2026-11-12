"""ShopStream API — Python standard library only (+ a Redis server for the cache).

GET  /health
GET  /products/<id>      product page, cached in Redis (cache.py)
GET  /stats              cache hits, misses and database reads
GET  /users/<id>/orders  a user's orders from their shard's follower; ?after=<lsn> = read your own write
GET  /replication        follower progress per shard
GET  /queue              receipt queue: waiting, pending, dead letters
POST /orders             {"user_id": "u1", "product_id": 7, "qty": 1}  rate-limited per user (ratelimit.py);
                         the receipt email is queued for worker.py (RECEIPTS=inline sends it in the request)
"""
import json
import os
import re
import socket
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import cache
import db
import ratelimit
import receipts
import resp
import shards
from resp import client

RECEIPTS = os.environ.get("RECEIPTS", "queue")  # "inline" = send in the request
PORT = int(os.environ.get("SHOP_PORT", "8080"))
_local = threading.local()


def conn():
    if not hasattr(_local, "conn"):
        _local.conn = db.connect()
    return _local.conn


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        # Send small responses immediately (no Nagle delay on keep-alive connections).
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def log_message(self, *args):  # keep the terminal quiet under load
        pass

    def send_json(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            return self.send_json(200, {"ok": True})
        if self.path == "/stats":
            return self.send_json(200, {"cache_mode": cache.MODE, **cache.stats})
        m = re.fullmatch(r"/users/([\w-]+)/orders(?:\?after=(\d+))?", self.path)
        if m:
            shard, orders, source = shards.orders_for(m.group(1), int(m.group(2) or 0))
            return self.send_json(200, {"shard": shard, "read_from": source, "orders": orders})
        if self.path == "/queue":
            r = resp.client()
            try:
                pending = r.call("XPENDING", receipts.STREAM, receipts.GROUP)[0]
                lag = dict(zip(*[iter(r.call("XINFO", "GROUPS", receipts.STREAM)[0])] * 2))["lag"]
            except RuntimeError:  # no worker has created the group yet
                pending, lag = 0, r.call("XLEN", receipts.STREAM)
            return self.send_json(200, {"mode": RECEIPTS, "stream_length": r.call("XLEN", receipts.STREAM),
                                        "waiting": lag, "pending": pending,
                                        "dead_letters": r.call("XLEN", receipts.DEAD)})
        if self.path == "/replication":
            return self.send_json(200, {"read_mode": shards.READ_MODE, **shards.stats,
                                        "lag": {s: shards.shard_conn(s).execute("SELECT MAX(lsn) FROM changelog").fetchone()[0] or 0
                                                for s in shards.RING.nodes} | {f"{s}_follower": shards.follower_lsn(s) for s in shards.RING.nodes}})
        m = re.fullmatch(r"/products/(\d+)", self.path)
        if m:
            page = cache.get_product(int(m.group(1)), lambda pid: db.product_page(conn(), pid))
            return self.send_json(200, page) if page else self.send_json(404, {"error": "not found"})
        self.send_json(404, {"error": "no route"})

    def do_POST(self):
        if self.path != "/orders":
            return self.send_json(404, {"error": "no route"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            ok, retry_after = ratelimit.allow(str(body["user_id"]))
            if not ok:
                self.send_response(429)
                self.send_header("Retry-After", str(retry_after))
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            order_id = db.place_order(conn(), str(body["user_id"]), int(body["product_id"]), int(body.get("qty", 1)))
        except (ValueError, KeyError):
            return self.send_json(400, {"error": "need user_id and product_id"})
        except sqlite3.OperationalError:  # database busy: tell the client to retry
            conn().rollback()
            return self.send_json(503, {"error": "busy, retry"})
        if order_id is None:
            return self.send_json(409, {"error": "out of stock"})
        order = {**order_id, "user_id": str(body["user_id"]), "product_id": int(body["product_id"])}
        if RECEIPTS == "inline":
            try:
                receipts.send_receipt(order)  # the customer waits for the email provider
            except receipts.SendError:
                pass  # the order is placed; the receipt is lost
        else:
            receipts.enqueue(order)  # a worker sends it; the customer doesn't wait
        self.send_json(201, order_id)


if __name__ == "__main__":
    db.seed()
    ThreadingHTTPServer.request_queue_size = 128  # default is 5: bursts would wait for TCP retries
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    server.daemon_threads = True
    print(f"ShopStream listening on http://127.0.0.1:{PORT}", flush=True)
    server.serve_forever()
