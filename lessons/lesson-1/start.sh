#!/usr/bin/env bash
# ShopStream — Lesson 1: Load-test the shop until it falls over
# Writes the lesson-1 code into ./shopstream (same folder for every lesson),
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

# ---- the code (app.py, db.py, load.py) ----
say "Writing lesson-1 code to $APP"
rm -f *.py
cat > app.py <<'SHOPSTREAM_EOF'
"""ShopStream API — Python standard library only.

GET  /health
GET  /products/<id>      product page (slow: scans reviews)
POST /orders             {"user_id": "u1", "product_id": 7, "qty": 1}
"""
import json
import os
import re
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import db

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
        m = re.fullmatch(r"/products/(\d+)", self.path)
        if m:
            page = db.product_page(conn(), int(m.group(1)))
            return self.send_json(200, page) if page else self.send_json(404, {"error": "not found"})
        self.send_json(404, {"error": "no route"})

    def do_POST(self):
        if self.path != "/orders":
            return self.send_json(404, {"error": "no route"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            order_id = db.place_order(conn(), str(body["user_id"]), int(body["product_id"]), int(body.get("qty", 1)))
        except (ValueError, KeyError):
            return self.send_json(400, {"error": "need user_id and product_id"})
        if order_id is None:
            return self.send_json(409, {"error": "out of stock"})
        self.send_json(201, {"order_id": order_id})


if __name__ == "__main__":
    db.seed()
    ThreadingHTTPServer.request_queue_size = 128  # default is 5: bursts would wait for TCP retries
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    server.daemon_threads = True
    print(f"ShopStream listening on http://127.0.0.1:{PORT}", flush=True)
    server.serve_forever()
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
    cur = conn.execute(
        "UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?", (qty, product_id, qty)
    )
    if cur.rowcount == 0:
        conn.rollback()
        return None
    order_id = conn.execute(
        "INSERT INTO orders (user_id, product_id, qty) VALUES (?, ?, ?)", (user_id, product_id, qty)
    ).lastrowid
    conn.commit()
    return order_id


if __name__ == "__main__":
    seed()
    print("seeded", DB_PATH)
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

# ---- verify.sh -------------------------------------------------------------
cat > "$ROOT/verify.sh" <<'VERIFY_EOF'
#!/usr/bin/env bash
# ShopStream lesson 1 check: prints your completion code when everything works.
set -euo pipefail
cd "$(dirname "$0")/shopstream"
python3 -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=2)' 2>/dev/null || { echo "The app is not running: run ./start.sh first."; exit 1; }
python3 - <<'EOF'
import json, urllib.request as u
p = json.load(u.urlopen("http://127.0.0.1:8080/products/7"))
assert p["reviews"] > 0, "product page has no reviews"
EOF
python3 load.py --users 10 --seconds 2 | awk 'NR==2 { if ($5 != 0) { print "errors under load: " $5; exit 1 } }'
echo
echo "PASS  Lesson 1 complete. Your completion code: SHOP1-LOAD-7K2Q"
VERIFY_EOF
chmod +x "$ROOT/verify.sh"

say "1/3  Seeding 2,000 products and 200,000 reviews (first run takes a few seconds)"
fresh_data
start_app
curl_json() { python3 -c 'import json,sys,urllib.request as u; print(json.dumps(json.load(u.urlopen(sys.argv[1])), indent=1))' "$1"; }
curl_json http://127.0.0.1:8080/products/7

say "2/3  Load test: 1, 5, 10, 25 and 50 concurrent users, 4 seconds each"
python3 load.py --users 1,5,10,25,50 --seconds 4 | tee .run/results-lesson1.txt

say "3/3  Break it: 200 users"
python3 load.py --users 200 --seconds 4 | tee -a .run/results-lesson1.txt

say "Done. ShopStream is running on http://127.0.0.1:8080"
echo "Run ./verify.sh for your completion code, ./stop.sh when you are finished."
