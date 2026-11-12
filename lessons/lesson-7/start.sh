#!/usr/bin/env bash
# ShopStream — Lesson 7: Defend your design in a 45-minute interview
# Writes the lesson-7 code into ./shopstream (same folder for every lesson),
# runs it, measures it, and creates verify.sh. Stop everything with ./stop.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
APP="$ROOT/shopstream"
mkdir -p "$APP/.run"
cd "$APP"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# ---- prerequisites -------------------------------------------------------
command -v python3 >/dev/null || { echo "Python 3.10+ is required (python3 not found)."; exit 1; }
python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))' || { echo "Python 3.10+ is required, found $(python3 -V)."; exit 1; }
command -v redis-server >/dev/null || { echo "Redis is required: 'sudo apt install redis-server' (Linux/WSL) or 'brew install redis' (macOS)."; exit 1; }
if ! redis-cli -p 6390 ping >/dev/null 2>&1; then
  redis-server --port 6390 --daemonize yes --save "" --appendonly no --pidfile "$APP/.run/redis.pid" >/dev/null
  sleep 0.5
fi
redis-cli -p 6390 ping >/dev/null || { echo "Could not start Redis on port 6390."; exit 1; }

# ---- helpers ---------------------------------------------------------------
stop_bg() {
  for f in .run/app.pid .run/replica.pid .run/worker.pid; do
    [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  sleep 0.5
}
start_app() {  # usage: start_app [ENV=VALUE ...]
  env "$@" python3 app.py > .run/app.log 2>&1 &
  echo $! > .run/app.pid
  for _ in $(seq 1 100); do
    python3 -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=1)' 2>/dev/null && return 0
    sleep 0.2
  done
  echo "The app did not start. Last lines of .run/app.log:"; tail -20 .run/app.log; exit 1
}
fresh_data() {
  stop_bg
  rm -f shop.db shop.db-* orders-*.db orders-*.db-* shards.json replica_lag.json
  if command -v redis-cli >/dev/null && redis-cli -p 6390 ping >/dev/null 2>&1; then redis-cli -p 6390 flushall >/dev/null; fi
}

# ---- the code (app.py, botdemo.py, cache.py, checkout.py, db.py, fill.py, herd.py, load.py, queue_demo.py, ratelimit.py, receipts.py, replica.py, reshard.py, resp.py, ring.py, shards.py, stale.py, system_check.py, worker.py, DESIGN.md) ----
say "Writing lesson-7 code to $APP"
rm -f *.py
cat > app.py <<'SHOPSTREAM_EOF'
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
SHOPSTREAM_EOF
cat > botdemo.py <<'SHOPSTREAM_EOF'
"""A scalper bot vs five real shoppers, all buying product 42 (1,000 in stock).

The bot fires orders from 20 threads as fast as it can; each real shopper
places one order every 2 seconds. After 10 seconds: who got the stock?
"""
import http.client
import json
import threading
import time

SECONDS = 10


def post(conn, user):
    t0 = time.perf_counter()
    conn.request("POST", "/orders", json.dumps({"user_id": user, "product_id": 42, "qty": 1}),
                 {"Content-Type": "application/json"})
    r = conn.getresponse()
    r.read()
    return r.status, (time.perf_counter() - t0) * 1000


def run(user, stop_at, pause, out):
    c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=30)
    while time.perf_counter() < stop_at:
        try:
            out.append(post(c, user))
        except OSError:  # connection dropped under load: count it, reconnect
            out.append((0, 0.0))
            c.close()
            c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=30)
        if pause:
            time.sleep(pause)


def stock():
    c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=10)
    c.request("GET", "/products/42")
    return json.loads(c.getresponse().read())["stock"]


if __name__ == "__main__":
    stop_at = time.perf_counter() + SECONDS
    bot, real = [], []
    threads = [threading.Thread(target=run, args=("bot", stop_at, 0, bot)) for _ in range(20)]
    threads += [threading.Thread(target=run, args=(f"shopper{i}", stop_at, 2.0, real)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    bot_ok = sum(1 for s, _ in bot if s == 201)
    real_ok = sum(1 for s, _ in real if s == 201)
    lat = sorted(ms for s, ms in real if s)
    print(f"bot:      {len(bot):>6} attempts, {bot_ok:>5} orders, {sum(1 for s,_ in bot if s == 429):>6} rejected (429)")
    print(f"shoppers: {len(real):>6} attempts, {real_ok:>5} orders, {sum(1 for s,_ in real if s == 409):>6} out of stock")
    print(f"shoppers' median order: {lat[len(lat)//2]:.0f} ms, slowest: {lat[-1]:.0f} ms | dropped connections: {sum(1 for s,_ in bot+real if s == 0)} | stock left: {stock()}")
SHOPSTREAM_EOF
cat > cache.py <<'SHOPSTREAM_EOF'
"""Cache-aside for product pages, with two ways to handle expiry.

CACHE_MODE=naive  every miss goes to the database (thundering herd on expiry)
CACHE_MODE=safe   jittered TTL + a short lock so only one request rebuilds a key
"""
import json
import os
import random
import threading
import time

from resp import client

MODE = os.environ.get("CACHE_MODE", "safe")
TTL = int(os.environ.get("CACHE_TTL", "30"))
stats = {"hits": 0, "misses": 0, "db_reads": 0}
_lock = threading.Lock()


def _count(key):
    with _lock:
        stats[key] += 1


def get_product(product_id, load_from_db):
    r = client()
    key = f"product:{product_id}"
    cached = r.call("GET", key)
    if cached is not None:
        _count("hits")
        return json.loads(cached)
    _count("misses")

    if MODE == "naive":
        page = load_from_db(product_id)
        _count("db_reads")
        if page:
            r.call("SET", key, json.dumps(page), "EX", TTL)
        return page

    # safe: one request takes a 2-second rebuild lock; the others wait for it
    if r.call("SET", f"lock:{key}", "1", "NX", "PX", 2000) == "OK":
        try:
            cached = r.call("GET", key)  # someone may have rebuilt it between our GET and our lock
            if cached is not None:
                _count("hits")
                return json.loads(cached)
            page = load_from_db(product_id)
            _count("db_reads")
            if page:
                ttl = TTL + random.randint(0, max(1, TTL // 5))  # jitter: hot keys don't expire together
                r.call("SET", key, json.dumps(page), "EX", ttl)
            return page
        finally:
            r.call("DEL", f"lock:{key}")
    for _ in range(40):  # wait up to ~2 s for the rebuild
        time.sleep(0.05)
        cached = r.call("GET", key)
        if cached is not None:
            _count("hits")
            return json.loads(cached)
    page = load_from_db(product_id)  # lock holder died: fall back to the database
    _count("db_reads")
    return page
SHOPSTREAM_EOF
cat > checkout.py <<'SHOPSTREAM_EOF'
"""Time 40 checkouts, one after another, as different users."""
import http.client
import json
import statistics
import sys
import time

c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=30)
run, times = int(time.time() * 1000), []
for i in range(int(sys.argv[1]) if len(sys.argv) > 1 else 40):
    t = time.perf_counter()
    c.request("POST", "/orders", json.dumps({"user_id": f"buyer{run}-{i}", "product_id": i % 2000 + 1}),
              {"Content-Type": "application/json"})
    r = c.getresponse(); r.read()
    times.append((time.perf_counter() - t) * 1000)
    assert r.status == 201, r.status
times.sort()
c.request("GET", "/queue"); q = json.loads(c.getresponse().read())
print(f"receipts {q['mode']:6}: checkout p50 {statistics.median(times):6.1f} ms, p99 {times[int(len(times)*0.99)]:6.1f} ms; "
      f"queue waiting {q['waiting']}, pending {q['pending']}")
SHOPSTREAM_EOF
cat > db.py <<'SHOPSTREAM_EOF'
"""ShopStream storage: one SQLite file, seeded with products and reviews."""
import os
import random
import sqlite3

DB_PATH = os.environ.get("SHOP_DB", "shop.db")


def connect(path=DB_PATH):
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def seed(path=DB_PATH, products=2_000, reviews=200_000):
    """Create tables and fill them once. Reviews have no index on product_id
    on purpose: the product page has to scan them, like a slow real query."""
    conn = connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL, price_cents INTEGER NOT NULL, stock INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL, stars INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, product_id INTEGER NOT NULL,
            qty INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
        """
    )
    if conn.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 0:
        rng = random.Random(42)
        conn.executemany(
            "INSERT INTO products (id, name, price_cents, stock) VALUES (?, ?, ?, ?)",
            [(i, f"Product {i}", rng.randint(199, 99_999), 1_000) for i in range(1, products + 1)],
        )
        conn.executemany(
            "INSERT INTO reviews (product_id, stars) VALUES (?, ?)",
            [(rng.randint(1, products), rng.randint(1, 5)) for _ in range(reviews)],
        )
        conn.commit()
    conn.close()


def product_page(conn, product_id):
    """The expensive read: product row plus its rating, computed from reviews."""
    row = conn.execute(
        "SELECT id, name, price_cents, stock FROM products WHERE id = ?", (product_id,)
    ).fetchone()
    if row is None:
        return None
    stats = conn.execute(
        "SELECT COUNT(*) AS n, ROUND(AVG(stars), 2) AS avg FROM reviews WHERE product_id = ?",
        (product_id,),
    ).fetchone()
    return {**dict(row), "reviews": stats["n"], "rating": stats["avg"]}


def place_order(conn, user_id, product_id, qty):
    """Take stock in the catalog database, then write the order to its shard.

    These are two databases, so this is not one transaction: if the shard
    write fails we put the stock back (a compensating action)."""
    import uuid

    import shards

    cur = conn.execute(
        "UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?", (qty, product_id, qty)
    )
    if cur.rowcount == 0:
        conn.rollback()
        return None
    conn.commit()
    order_id = uuid.uuid4().hex[:12]
    try:
        shard, lsn = shards.insert_order(order_id, user_id, product_id, qty)
    except Exception:
        conn.execute("UPDATE products SET stock = stock + ? WHERE id = ?", (qty, product_id))
        conn.commit()
        raise
    return {"order_id": order_id, "shard": shard, "lsn": lsn}


if __name__ == "__main__":
    seed()
    print("seeded", DB_PATH)
SHOPSTREAM_EOF
cat > fill.py <<'SHOPSTREAM_EOF'
"""Write a history of orders straight to the shards: USERS users x 10 orders."""
import random
import sys
import uuid

import shards

USERS = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
rng = random.Random(7)
for u in range(USERS):
    for _ in range(10):
        shards.insert_order(uuid.uuid4().hex[:12], f"user{u}", rng.randint(1, 2000), 1)
print(f"wrote {USERS * 10} orders")
SHOPSTREAM_EOF
cat > herd.py <<'SHOPSTREAM_EOF'
"""Thundering herd demo: 50 shoppers open the same hot product the moment
its cache entry disappears. Count how many of them hit the database."""
import http.client
import json
import threading

from resp import Redis

r = Redis()


def stats():
    c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=10)
    c.request("GET", "/stats")
    return json.loads(c.getresponse().read())


def shopper(barrier):
    c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=10)
    barrier.wait()
    c.request("GET", "/products/1")
    c.getresponse().read()


if __name__ == "__main__":
    r.call("DEL", "product:1")  # the hot key just expired
    before = stats()["db_reads"]
    barrier = threading.Barrier(50)
    threads = [threading.Thread(target=shopper, args=(barrier,)) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    after = stats()
    print(f"cache mode: {after['cache_mode']} -> 50 requests caused {after['db_reads'] - before} database reads")
SHOPSTREAM_EOF
cat > load.py <<'SHOPSTREAM_EOF'
"""Tiny load generator: N concurrent users hammer one URL for a few seconds.

python3 load.py --url http://127.0.0.1:8080/products/ --users 1,5,10,25,50 --seconds 5
"""
import argparse
import http.client
import random
import statistics
import threading
import time
from urllib.parse import urlparse


def worker(url, stop_at, results, ids):
    u = urlparse(url)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
    while time.perf_counter() < stop_at:
        path = u.path + (str(random.choice(ids)) if u.path.endswith("/") else "")
        t0 = time.perf_counter()
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            resp.read()
            ok = resp.status < 500
        except Exception:
            ok = False
            conn.close()
            conn = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
        results.append(((time.perf_counter() - t0) * 1000, ok))
    conn.close()


def run(url, users, seconds, ids):
    results, stop_at = [], time.perf_counter() + seconds
    threads = [threading.Thread(target=worker, args=(url, stop_at, results, ids)) for _ in range(users)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    lat = sorted(r[0] for r in results if r[1])
    errors = sum(1 for r in results if not r[1])
    if not lat:
        return {"users": users, "rps": 0, "p50": 0, "p99": 0, "errors": errors}
    return {
        "users": users,
        "rps": round(len(lat) / seconds),
        "p50": round(statistics.median(lat), 1),
        "p99": round(lat[min(len(lat) - 1, int(len(lat) * 0.99))], 1),
        "errors": errors,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080/products/")
    ap.add_argument("--users", default="1,5,10,25,50")
    ap.add_argument("--seconds", type=float, default=5)
    ap.add_argument("--hot", type=int, default=200, help="pick product ids from 1..hot")
    a = ap.parse_args()
    ids = list(range(1, a.hot + 1))
    print(f"{'users':>6} {'req/s':>7} {'p50 ms':>8} {'p99 ms':>8} {'errors':>7}")
    for n in [int(x) for x in a.users.split(",")]:
        r = run(a.url, n, a.seconds, ids)
        print(f"{r['users']:>6} {r['rps']:>7} {r['p50']:>8} {r['p99']:>8} {r['errors']:>7}", flush=True)
SHOPSTREAM_EOF
cat > queue_demo.py <<'SHOPSTREAM_EOF'
"""Break the receipt workers on purpose and check nobody is mailed twice.

1. 30 orders; the email provider fails 20% of the time; plus one poison order.
2. Worker A crashes after sending its 4th receipt but before XACK; worker B
   takes the job over and must not send that receipt again.
Then: every good order got exactly one receipt; the poison one is a dead letter.
"""
import http.client
import json
import os
import subprocess
import sys
import time

import resp

c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=30)
r = resp.Redis()


def post(user, product):
    c.request("POST", "/orders", json.dumps({"user_id": user, "product_id": product}), {"Content-Type": "application/json"})
    res = c.getresponse()
    return json.loads(res.read())


r.call("DEL", "receipts", "receipts:dead")  # start from an empty queue
run = int(time.time())
orders = [post(f"q{run}-{i}", i + 1)["order_id"] for i in range(30)]
poison = post("poison", 99)["order_id"]
print(f"placed 30 orders + 1 poison order")

env = {**os.environ, "FAIL_RATE": "0.2", "CLAIM_MS": "1000"}
log = open("workers.log", "w")
a = subprocess.Popen([sys.executable, "worker.py", "worker-A"], env={**env, "CRASH_AFTER": "3"}, stdout=log)
time.sleep(0.3)
b = subprocess.Popen([sys.executable, "worker.py", "worker-B"], env=env, stdout=log)
deadline = time.time() + 60
while time.time() < deadline:
    c.request("GET", "/queue"); q = json.loads(c.getresponse().read())
    if q["waiting"] == 0 and q["pending"] == 0:
        break
    time.sleep(0.5)
b.terminate(); a.wait(); b.wait()

sent = [int(r.call("GET", f"receipts_sent:{o}") or 0) for o in orders]
print(f"queue after: waiting {q['waiting']}, pending {q['pending']}, dead letters {q['dead_letters']}")
print(f"good orders with exactly one receipt: {sent.count(1)}/30; sent twice or more: {sum(s > 1 for s in sent)}; never: {sent.count(0)}")
dead = r.call("XRANGE", "receipts:dead", "-", "+")
print("dead letters:", ["poison order" if dict(zip(f[::2], f[1::2]))["order_id"] == poison else "flaky order" for _, f in dead])
log.close()
lines = open("workers.log").read().splitlines()
print(f"failed sends retried: {sum('failed' in l for l in lines)}; " + next((l for l in lines if "crashed" in l), "no crash"))
print("RESULT", "ok" if sent.count(1) == 30 and len(dead) == 1 else "check")
SHOPSTREAM_EOF
cat > ratelimit.py <<'SHOPSTREAM_EOF'
"""Per-user token bucket in Redis.

Each user has a bucket of CAPACITY tokens that refills at RATE tokens per
second. An order costs one token; an empty bucket means HTTP 429. The check
and the update run as one Lua script, so two requests can't both take the
last token.

RATE_LIMIT=off turns it off (for the "before" measurement).
"""
import os
import time

from resp import client

ENABLED = os.environ.get("RATE_LIMIT", "on") != "off"
CAPACITY = int(os.environ.get("RL_CAPACITY", "5"))
RATE = float(os.environ.get("RL_RATE", "1"))  # tokens per second

TOKEN_BUCKET = """
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local bucket   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens   = tonumber(bucket[1]) or capacity
local ts       = tonumber(bucket[2]) or now
tokens = math.min(capacity, tokens + (now - ts) * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)
local wait_ms = 0
if allowed == 0 then wait_ms = math.ceil((1 - tokens) / rate * 1000) end
return {allowed, wait_ms}
"""


def allow(user_id):
    """Returns (allowed, retry_after_seconds)."""
    if not ENABLED:
        return True, 0
    allowed, wait_ms = client().call(
        "EVAL", TOKEN_BUCKET, 1, f"rl:{user_id}", CAPACITY, RATE, f"{time.time():.3f}"
    )
    return allowed == 1, max(1, -(-wait_ms // 1000))
SHOPSTREAM_EOF
cat > receipts.py <<'SHOPSTREAM_EOF'
"""Send an order receipt — the slow, flaky work we want off the request path.

The "email provider" takes SEND_MS per message and fails FAIL_RATE of the
time. Orders from user "poison" always fail (a message that can never succeed).
"""
import os
import random
import time

import resp

SEND_MS = int(os.environ.get("SEND_MS", "300"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))
STREAM, GROUP, DEAD = "receipts", "mailers", "receipts:dead"


class SendError(Exception):
    pass


def send_receipt(order):
    time.sleep(SEND_MS / 1000)
    if order["user_id"] == "poison" or random.random() < FAIL_RATE:
        raise SendError(f"provider rejected receipt for {order['order_id']}")
    resp.client().call("INCR", f"receipts_sent:{order['order_id']}")  # how many times this customer got mail


def enqueue(order):
    """Add a receipt job to the stream. Returns the job id."""
    fields = []
    for k, v in order.items():
        fields += [k, v]
    return resp.client().call("XADD", STREAM, "MAXLEN", "~", "100000", "*", *fields)
SHOPSTREAM_EOF
cat > replica.py <<'SHOPSTREAM_EOF'
"""Ship each shard leader's changelog to its follower, every INTERVAL seconds.

Real databases stream their write-ahead log continuously; polling makes the
lag easy to see. Lag = time from the leader's commit to the follower's apply.
"""
import json
import os
import statistics
import time

import shards

INTERVAL = float(os.environ.get("REPLICA_INTERVAL", "0.5"))


def apply_batch(shard, lags):
    leader, follower = shards.connect(shard), shards.connect_follower(shard)
    done = follower.execute("SELECT applied_lsn FROM replica_state").fetchone()[0]
    rows = leader.execute("SELECT lsn, ts, op, row FROM changelog WHERE lsn > ? ORDER BY lsn LIMIT 5000", (done,)).fetchall()
    if rows:
        with follower:
            for r in rows:
                o = json.loads(r["row"])
                follower.execute("INSERT OR IGNORE INTO orders (id, user_id, product_id, qty) VALUES (:id, :user_id, :product_id, :qty)", o)
            follower.execute("UPDATE replica_state SET applied_lsn = ?", (rows[-1]["lsn"],))
        now = time.time()
        lags += [(now - r["ts"]) * 1000 for r in rows]
    leader.close(); follower.close()
    return len(rows)


if __name__ == "__main__":
    lags, last_report = [], time.time()
    print(f"replicating every {INTERVAL}s", flush=True)
    while True:
        for s in shards.load_ring().nodes:
            apply_batch(s, lags)
        if lags and time.time() - last_report > 5:
            lags.sort()
            print(f"applied {len(lags)} writes; lag p50 {statistics.median(lags):.0f} ms, p99 {lags[int(len(lags)*0.99)]:.0f} ms", flush=True)
            with open("replica_lag.json", "w") as f:
                json.dump({"n": len(lags), "p50": statistics.median(lags), "p99": lags[int(len(lags)*0.99)]}, f)
            lags, last_report = [], time.time()
        time.sleep(INTERVAL)
SHOPSTREAM_EOF
cat > reshard.py <<'SHOPSTREAM_EOF'
"""Add a shard and move only the orders whose owner now maps elsewhere.

python3 reshard.py --add shard3          compare ring vs hash % N, then migrate
"""
import argparse
import json
import sqlite3

import shards
from ring import Ring, mod_node


def all_orders(names):
    rows = []
    for name in names:
        c = shards.connect(name)
        rows += [(name, dict(r)) for r in c.execute("SELECT * FROM orders")]
        c.close()
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--add", required=True)
    a = ap.parse_args()
    cfg = json.load(open(shards.CONFIG))
    old, new = cfg["shards"], cfg["shards"] + [a.add]
    rows = all_orders(old)
    users = {r["user_id"] for _, r in rows}
    ring_old, ring_new = Ring(old), Ring(new)
    moved_ring = [(src, r) for src, r in rows if ring_new.node_for(r["user_id"]) != src]
    moved_users_mod = sum(1 for u in users if mod_node(u, len(old)) != mod_node(u, len(new)))
    moved_users_ring = sum(1 for u in users if ring_old.node_for(u) != ring_new.node_for(u))
    print(f"{len(rows)} orders from {len(users)} users on {len(old)} shards; adding {a.add}")
    print(f"  hash % N would move {moved_users_mod / len(users):6.1%} of users")
    print(f"  the ring moves      {moved_users_ring / len(users):6.1%} of users ({len(moved_ring)} orders)")
    # copy first, switch the ring, then delete from the old shards
    for src, r in moved_ring:
        dst = shards.connect(ring_new.node_for(r["user_id"]))
        dst.execute("INSERT OR IGNORE INTO orders (id, user_id, product_id, qty, created_at) VALUES (?, ?, ?, ?, ?)",
                    (r["id"], r["user_id"], r["product_id"], r["qty"], r["created_at"]))
        dst.commit(); dst.close()
    cfg["shards"] = new
    json.dump(cfg, open(shards.CONFIG, "w"))
    for src, r in moved_ring:
        c = shards.connect(src)
        c.execute("DELETE FROM orders WHERE id = ?", (r["id"],)); c.commit(); c.close()
    after = all_orders(new)
    counts = {n: sum(1 for s, _ in after if s == n) for n in new}
    print(f"  after: {len(after)} orders, per shard {counts}")
SHOPSTREAM_EOF
cat > resp.py <<'SHOPSTREAM_EOF'
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
SHOPSTREAM_EOF
cat > ring.py <<'SHOPSTREAM_EOF'
"""Consistent hashing with virtual nodes.

Each database owns many points ("virtual nodes") on a ring of 2**64 hash
values. A key belongs to the first point clockwise from its own hash.
Adding a database only takes over the arcs next to its new points, so only
about 1/N of the keys move.
"""
import bisect
import hashlib


def h(key: str) -> int:
    return int.from_bytes(hashlib.md5(key.encode()).digest()[:8], "big")


class Ring:
    def __init__(self, nodes, vnodes=100):
        self.vnodes = vnodes
        self._points, self._owner = [], {}
        for n in nodes:
            self.add(n)

    def add(self, node):
        for i in range(self.vnodes):
            p = h(f"{node}#{i}")
            bisect.insort(self._points, p)
            self._owner[p] = node

    @property
    def nodes(self):
        return sorted(set(self._owner.values()))

    def node_for(self, key):
        i = bisect.bisect(self._points, h(key)) % len(self._points)
        return self._owner[self._points[i]]


def mod_node(key, n):
    """The naive alternative: hash(key) % N."""
    return f"shard{h(key) % n}"


if __name__ == "__main__":
    keys = [f"order:{i}" for i in range(100_000)]
    before = {k: mod_node(k, 3) for k in keys}
    after = {k: mod_node(k, 4) for k in keys}
    moved_mod = sum(before[k] != after[k] for k in keys)
    for v in (1, 10, 100, 200):
        r3 = Ring(["db0", "db1", "db2"], vnodes=v)
        a3 = {k: r3.node_for(k) for k in keys}
        r4 = Ring(["db0", "db1", "db2"], vnodes=v); r4.add("db3")
        a4 = {k: r4.node_for(k) for k in keys}
        moved = sum(a3[k] != a4[k] for k in keys)
        load = [sum(1 for k in keys if a4[k] == f"db{i}") for i in range(4)]
        spread = (max(load) - min(load)) / (len(keys) / 4) * 100
        print(f"vnodes={v:>3}: moved {moved/len(keys)*100:5.1f}% | 4-node load {load} | max-min spread {spread:4.1f}% of fair share")
    print(f"hash % N (3 -> 4): moved {moved_mod/len(keys)*100:5.1f}%")
SHOPSTREAM_EOF
cat > shards.py <<'SHOPSTREAM_EOF'
"""Orders live in N SQLite shards, chosen by user_id on a consistent-hash ring.

Lesson 5: every shard is a leader with a follower copy. Writes go to the
leader and append to its changelog in the same transaction; replica.py
ships the changelog to the follower. Reads go to the follower, unless the
caller has just written something the follower hasn't applied yet
(read-your-own-writes).

shards.json lists the shards ({"shards": ["shard0", "shard1", "shard2"]}).
All of a user's orders sit on one shard, so "my orders" reads one database.
"""
import json
import os
import sqlite3
import threading

from ring import Ring

CONFIG = os.environ.get("SHARDS_FILE", "shards.json")
_local = threading.local()


def load_ring():
    if not os.path.exists(CONFIG):
        with open(CONFIG, "w") as f:
            json.dump({"shards": ["shard0", "shard1", "shard2"]}, f)
    with open(CONFIG) as f:
        return Ring(json.load(f)["shards"])


def connect(shard):
    conn = sqlite3.connect(f"orders-{shard}.db", timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY, user_id TEXT NOT NULL, product_id INTEGER NOT NULL,
            qty INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS orders_user ON orders(user_id)")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS changelog (
            lsn INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, op TEXT NOT NULL, row TEXT NOT NULL)"""
    )
    conn.execute("CREATE TABLE IF NOT EXISTS replica_state (id INTEGER PRIMARY KEY CHECK (id = 1), applied_lsn INTEGER NOT NULL)")
    conn.execute("INSERT OR IGNORE INTO replica_state (id, applied_lsn) VALUES (1, 0)")
    conn.commit()
    return conn


def connect_follower(shard):
    return connect(f"{shard}-follower")


def shard_conn(shard, follower=False):
    cache = getattr(_local, "conns", None)
    if cache is None:
        cache = _local.conns = {}
    key = (shard, follower)
    if key not in cache:
        cache[key] = connect_follower(shard) if follower else connect(shard)
    return cache[key]


READ_MODE = os.environ.get("READ_MODE", "ryw")  # "follower" = always read the follower
stats = {"follower_reads": 0, "leader_reads": 0}


RING = load_ring()


def insert_order(order_id, user_id, product_id, qty):
    """Write to the shard leader. Returns (shard, lsn) — the lsn is the
    position of this write in the leader's changelog."""
    import time

    shard = RING.node_for(user_id)
    c = shard_conn(shard)
    row = {"id": order_id, "user_id": user_id, "product_id": product_id, "qty": qty}
    with c:  # one transaction: the order and its changelog entry
        c.execute("INSERT INTO orders (id, user_id, product_id, qty) VALUES (:id, :user_id, :product_id, :qty)", row)
        lsn = c.execute("INSERT INTO changelog (ts, op, row) VALUES (?, 'insert', ?)",
                        (time.time(), json.dumps(row))).lastrowid
    return shard, lsn


def follower_lsn(shard):
    return shard_conn(shard, follower=True).execute("SELECT applied_lsn FROM replica_state").fetchone()[0]


def orders_for(user_id, min_lsn=0):
    """Read from the follower; if the caller needs a write the follower
    hasn't applied yet (min_lsn), read the leader instead."""
    shard = RING.node_for(user_id)
    use_leader = READ_MODE == "ryw" and min_lsn and follower_lsn(shard) < min_lsn
    stats["leader_reads" if use_leader else "follower_reads"] += 1
    rows = shard_conn(shard, follower=not use_leader).execute(
        "SELECT id, product_id, qty, created_at FROM orders WHERE user_id = ? ORDER BY created_at", (user_id,)
    ).fetchall()
    return shard, [dict(r) for r in rows], ("leader" if use_leader else "follower")
SHOPSTREAM_EOF
cat > stale.py <<'SHOPSTREAM_EOF'
"""Place an order, then immediately open "my orders". Do you see it?

Runs 100 times with fresh users (so the rate limiter stays out of the way).
"""
import http.client
import json
import time

c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=10)


def call(method, path, body=None):
    c.request(method, path, json.dumps(body) if body else None, {"Content-Type": "application/json"})
    r = c.getresponse()
    return r.status, json.loads(r.read() or b"{}")


stale = leader = 0
run = int(time.time())
for i in range(100):
    user = f"reader{run}-{i}"
    status, order = call("POST", "/orders", {"user_id": user, "product_id": (i % 2000) + 1})
    _, mine = call("GET", f"/users/{user}/orders?after={order['lsn']}")
    seen = any(o["id"] == order["order_id"] for o in mine["orders"])
    stale += not seen
    leader += mine["read_from"] == "leader"
    time.sleep(0.02)
_, rep = call("GET", "/replication")
print(f"read mode {rep['read_mode']}: {stale}/100 reads missed the order just placed; {leader}/100 served by the leader")
SHOPSTREAM_EOF
cat > system_check.py <<'SHOPSTREAM_EOF'
"""End-to-end check of the whole ShopStream design, one pattern at a time.

Needs app.py, replica.py and worker.py running. Prints a scorecard you can
paste under your architecture diagram.
"""
import http.client
import json
import subprocess
import sys
import time

import resp
import shards

c = http.client.HTTPConnection("127.0.0.1", 8080, timeout=30)
r = resp.Redis()
run = int(time.time())
results = []


def call(method, path, body=None):
    c.request(method, path, json.dumps(body) if body else None, {"Content-Type": "application/json"})
    res = c.getresponse()
    data = res.read()
    return res.status, (json.loads(data) if data else {})


def check(name, ok, detail):
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name:<22} {detail}")


# 1. Cache: the second read of a product is served from Redis.
_, before = call("GET", "/stats")
call("GET", "/products/77"); call("GET", "/products/77")
_, after = call("GET", "/stats")
check("cache-aside", after["hits"] > before["hits"], f"hits {before['hits']} -> {after['hits']}, hit rate {after['hits'] / max(1, after['hits'] + after['misses']):.1%}")

# 2. Rate limit: a burst of 8 orders from one user is cut off at the bucket size.
codes = [call("POST", "/orders", {"user_id": f"burst{run}", "product_id": 5})[0] for _ in range(8)]
check("rate limiter", codes.count(201) == 5 and codes.count(429) == 3, f"8 rapid orders -> {codes.count(201)} accepted, {codes.count(429)} x 429")

# 3. Sharding: the order lands on the shard the ring picks for its user.
user = f"check{run}"
_, order = call("POST", "/orders", {"user_id": user, "product_id": 9})
check("consistent hashing", order["shard"] == shards.load_ring().node_for(user), f"{user} -> {order['shard']} ({len(shards.load_ring().nodes)} shards)")

# 4. Read-your-own-writes: an immediate read with ?after=<lsn> sees the order.
_, mine = call("GET", f"/users/{user}/orders?after={order['lsn']}")
check("read-your-own-writes", any(o["id"] == order["order_id"] for o in mine["orders"]), f"read from the {mine['read_from']} right after the write")

# 5. Replication: within 3 s the follower has it too.
t0, seen = time.time(), False
while time.time() - t0 < 3 and not seen:
    _, plain = call("GET", f"/users/{user}/orders")
    seen = any(o["id"] == order["order_id"] for o in plain["orders"])
    time.sleep(0.05)
check("replication", seen, f"follower caught up in {(time.time() - t0) * 1000:.0f} ms")

# 6. Queue: the receipt is sent by a worker, exactly once.
t0 = time.time()
while time.time() - t0 < 5 and not r.call("GET", f"receipts_sent:{order['order_id']}"):
    time.sleep(0.05)
sent = int(r.call("GET", f"receipts_sent:{order['order_id']}") or 0)
check("async receipts", sent == 1, f"receipt sent {sent}x, {(time.time() - t0) * 1000:.0f} ms after checkout")

# 7. Load: product pages under 25 concurrent users.
out = subprocess.run([sys.executable, "load.py", "--users", "25", "--seconds", "3"], capture_output=True, text=True).stdout
row = [l.split() for l in out.splitlines() if l.strip().startswith("25")][0]
check("load (25 users)", float(row[3]) < 100 and row[4] == "0", f"{row[1]} req/s, p50 {row[2]} ms, p99 {row[3]} ms, {row[4]} errors")

print(f"\n{sum(results)}/{len(results)} checks passed")
sys.exit(0 if all(results) else 1)
SHOPSTREAM_EOF
cat > worker.py <<'SHOPSTREAM_EOF'
"""Receipt worker: a consumer in the "mailers" group of the "receipts" stream.

- New jobs:      XREADGROUP ... >        (each job goes to one worker)
- Done:          XACK                    (only after the receipt is sent)
- Failed/crashed jobs stay pending; after CLAIM_MS any worker takes them
  over with XAUTOCLAIM, which also counts the delivery.
- After MAX_TRIES deliveries a job goes to the dead-letter stream.
- Idempotent: a receipt already sent for an order is never sent again.
"""
import os
import socket
import sys
import time

import receipts
import resp

NAME = sys.argv[1] if len(sys.argv) > 1 else f"worker-{socket.gethostname()}-{os.getpid()}"
CLAIM_MS = int(os.environ.get("CLAIM_MS", "3000"))
MAX_TRIES = int(os.environ.get("MAX_TRIES", "5"))
CRASH_AFTER = int(os.environ.get("CRASH_AFTER", "0"))  # demo: die before acking job N+1
r = resp.Redis()


def setup():
    try:
        r.call("XGROUP", "CREATE", receipts.STREAM, receipts.GROUP, "0", "MKSTREAM")
    except RuntimeError as e:
        if "BUSYGROUP" not in str(e):
            raise


def as_dict(flat):
    return dict(zip(flat[::2], flat[1::2]))


def deliveries(job_id):
    info = r.call("XPENDING", receipts.STREAM, receipts.GROUP, job_id, job_id, 1)
    return info[0][3] if info else 0


def handle(job_id, order, done):
    tries = deliveries(job_id)
    if tries > MAX_TRIES:
        r.call("XADD", receipts.DEAD, "*", "job", job_id, "tries", tries - 1, *sum(order.items(), ()))
        r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)
        print(f"{NAME}: {job_id} dead-lettered after {tries - 1} tries", flush=True)
        return
    if r.call("EXISTS", f"receipt_done:{order['order_id']}"):
        r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)  # sent before we crashed: don't send twice
        return
    try:
        receipts.send_receipt(order)
    except receipts.SendError as e:
        print(f"{NAME}: try {tries} failed: {e}", flush=True)
        return  # stays pending; retried after CLAIM_MS
    r.call("SET", f"receipt_done:{order['order_id']}", 1, "EX", 86400)
    if CRASH_AFTER and done >= CRASH_AFTER:
        print(f"{NAME}: crashed after sending {job_id} but before XACK", flush=True)
        os._exit(1)
    r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)


def main():
    setup()
    print(f"{NAME}: waiting for receipts", flush=True)
    done = 0
    while True:
        # 1) take over jobs another worker failed or abandoned
        _, stolen, *_ = r.call("XAUTOCLAIM", receipts.STREAM, receipts.GROUP, NAME, CLAIM_MS, "0-0", "COUNT", 10)
        jobs = list(stolen or [])
        # 2) new jobs
        if not jobs:
            reply = r.call("XREADGROUP", "GROUP", receipts.GROUP, NAME, "COUNT", 10, "BLOCK", 1000,
                           "STREAMS", receipts.STREAM, ">")
            jobs = reply[0][1] if reply else []
        for job_id, flat in jobs:
            if flat is None:  # deleted while pending
                r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)
                continue
            handle(job_id, as_dict(flat), done)
            done += 1


if __name__ == "__main__":
    main()
SHOPSTREAM_EOF
cat > DESIGN.md <<'SHOPSTREAM_EOF'
# ShopStream — one-page design

## Requirements I designed for
- Product pages: read-heavy (100:1 reads to writes), p99 under 100 ms at peak.
- Checkout: never oversell; one bot must not starve real shoppers.
- Orders: grow past one database without a big-bang migration.
- "My orders" must show an order the moment it is placed.
- A slow or failing email provider must not slow checkout or lose receipts.

## Architecture
```
client ──> app.py (stateless HTTP, many threads; add instances behind a load balancer)
             │ GET /products/<id>  ──> Redis cache-aside (TTL 30 s ± jitter, rebuild lock) ──miss──> SQLite products/reviews
             │ POST /orders        ──> Redis token bucket (5 burst, 1/s per user) ──> stock decrement (conditional UPDATE)
             │                         ──> orders shard leader (consistent-hash ring, 100 vnodes) + changelog
             │                         ──> XADD receipts ──> worker.py (consumer group, retries, DLQ)
             │ GET /users/<id>/orders ──> shard follower; leader if follower LSN < client's last write LSN
replica.py ── ships each leader's changelog to its follower every 0.5 s
```

## Numbers I measured (2-core laptop, lessons 1–6)
| Change | Before | After |
|---|---|---|
| Cache on product pages (50 users) | 280 req/s, p99 514 ms | 5,256 req/s, p99 29 ms |
| Herd of 50 on one expired key | 9–11 DB reads | 1 DB read |
| Bot vs 5 shoppers, product with 1,000 units | shoppers got 5 of 25 tries | shoppers got 25 of 25; bot got 15 |
| Add a 4th shard (2,000 users) | hash % N moves 74% of users | ring moves 22% |
| "My orders" right after checkout | 100/100 stale from follower | 0/100 stale with read-your-own-writes |
| Checkout with a 300 ms email provider | p50 302 ms | p50 0.9 ms (queued) |

## Trade-offs I chose, and when I'd change them
- **Cache TTL 30 s**: prices/stock on the page can be 30 s old; checkout re-checks stock in the database, so no oversell. For flash sales, invalidate on write instead.
- **Token bucket per user**: simple, allows short bursts. A bot with many accounts needs per-IP/device limits too.
- **Shard by user_id**: "my orders" hits one shard. "All orders for product X" now fans out to every shard — keep a separate analytics copy for that.
- **Async replication**: fast writes, but a follower can lag (p50 263 ms, p99 503 ms here) and a leader crash can lose the last half-second. Sync replication would cost a network round-trip per write.
- **At-least-once queue + idempotency key**: a receipt is never lost; the dedupe key stops double emails after a crash. Exactly-once delivery to an outside provider isn't possible; idempotency is how we fake it.
- **Saga, not a distributed transaction**: stock and order live in different databases; a failed order write adds the stock back.

## What I'd do next
Move products to Postgres with a read replica; put two app instances behind a load balancer; Redis Sentinel or a managed Redis for failover; Kafka instead of Redis Streams when several services need the order events (next course).
SHOPSTREAM_EOF

say "1/2  Start the whole system: app, replica, receipt worker (20,000 orders of history)"
fresh_data
python3 fill.py 2000
python3 replica.py > .run/replica.log 2>&1 &
echo $! > .run/replica.pid
python3 worker.py worker-1 > .run/worker.log 2>&1 &
echo $! > .run/worker.pid
start_app
sleep 4

say "2/2  End-to-end check of every pattern"
python3 system_check.py | tee .run/results-lesson7.txt
echo
echo "Now open DESIGN.md: your one-page architecture and trade-off sheet."

# ---- verify.sh -------------------------------------------------------------
cat > "$ROOT/verify.sh" <<'VERIFY_EOF'
#!/usr/bin/env bash
# ShopStream lesson 7 check: prints your completion code when everything works.
set -euo pipefail
cd "$(dirname "$0")/shopstream"
python3 -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=2)' 2>/dev/null || { echo "The app is not running: run ./start.sh first."; exit 1; }
python3 system_check.py
echo
echo "PASS  Lesson 7 complete. Your completion code: SHOP1-DEFEND-8PZ1"
VERIFY_EOF
chmod +x "$ROOT/verify.sh"

say "Done. ShopStream is running on http://127.0.0.1:8080"
echo "Run ./verify.sh for your completion code, ./stop.sh when you are finished."
