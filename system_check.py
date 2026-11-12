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
