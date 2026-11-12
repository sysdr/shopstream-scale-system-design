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
