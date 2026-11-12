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
