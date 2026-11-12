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
