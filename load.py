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
