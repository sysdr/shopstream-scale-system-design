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
