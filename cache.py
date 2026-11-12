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
