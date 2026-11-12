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
