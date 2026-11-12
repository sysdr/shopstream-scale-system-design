# Scale ShopStream: System Design in 7 Builds (Python)

Source code for the free course **[Scale ShopStream](https://systemdrd.com/course/scale-shopstream-system-design-in-7-builds-python/)** on [systemdrd.com](https://systemdrd.com).

You take a small online-shop API, push it until it breaks, and fix it with the patterns large systems use. Every lesson has a failure you cause on purpose and a number you measure before and after.

**No `pip install`.** Plain Python 3.10+ (standard library only), SQLite and Redis.

## Lessons

| # | Lesson | What you measure | Run |
|---|---|---|---|
| 1 | [Load-test the shop until it falls over](https://systemdrd.com/lessons/day-1-load-test-the-shop-until-it-falls-over/) | 280 req/s ceiling, p99 514 ms at 50 users | `lessons/lesson-1` |
| 2 | [Add a cache and watch latency drop](https://systemdrd.com/lessons/day-2-add-a-cache-and-watch-latency-drop/) | 5,256 req/s; thundering herd 9–36 → 1 DB read | `lessons/lesson-2` |
| 3 | [Rate-limit checkout before bots do](https://systemdrd.com/lessons/day-3-rate-limit-checkout-before-bots-do/) | bot 995 → 15 orders; shoppers 25 of 25 | `lessons/lesson-3` |
| 4 | [Shard orders across three databases](https://systemdrd.com/lessons/day-4-shard-orders-across-three-databases/) | adding a shard: `hash % N` moves 74% of users, the ring 22% | `lessons/lesson-4` |
| 5 | [Replicate and read your own writes](https://systemdrd.com/lessons/day-5-replicate-and-read-your-own-writes/) | stale reads 100/100 → 0/100 | `lessons/lesson-5` |
| 6 | [Move slow work off the request path](https://systemdrd.com/lessons/day-6-move-slow-work-off-the-request-path/) | checkout p50 302 ms → 0.9 ms; 30/30 receipts sent once after a crash | `lessons/lesson-6` |
| 7 | [Defend your design in a 45-minute interview](https://systemdrd.com/lessons/day-7-defend-your-design-in-a-45-minute-interview/) | end-to-end check 7/7 | `lessons/lesson-7` |

## Quick start

Requirements: Python 3.10+, Redis 6.2+ (`sudo apt install redis-server` or `brew install redis`), Linux, macOS or WSL.

```bash
cd lessons/lesson-1
bash start.sh  # writes the lesson's code into ./shopstream, runs it and measures it
bash verify.sh  # checks it works and prints your completion code
bash stop.sh
```

Each `start.sh` writes the complete code for that lesson, so you can start at any lesson. It runs its own Redis on port 6390 and the app on port 8080.

## The finished system

The repository root holds the final code (lesson 7):

| File | Role |
|---|---|
| `app.py` | HTTP API (standard-library `ThreadingHTTPServer`) |
| `db.py` | products, stock, order placement (SQLite) |
| `resp.py` | a 60-line Redis client |
| `cache.py` | cache-aside with a single-flight rebuild lock |
| `ratelimit.py` | per-user token bucket (atomic Lua in Redis) |
| `ring.py`, `shards.py`, `reshard.py` | consistent hashing, sharded orders, resharding |
| `replica.py` | leader → follower changelog shipping; read-your-own-writes |
| `receipts.py`, `worker.py` | Redis Streams queue, retries, dead letters, idempotency |
| `load.py`, `herd.py`, `botdemo.py`, `stale.py`, `checkout.py`, `queue_demo.py`, `system_check.py` | experiments and checks |
| `DESIGN.md` | one-page design and trade-offs |

To run it directly: start Redis on port 6390 (`redis-server --port 6390`), then `python3 fill.py`, `python3 replica.py &`, `python3 worker.py &`, `python3 app.py &` and `python3 system_check.py`.

## Part of the free ShopStream path

This is Step 1 of a free learning path that grows the same system. See all free courses at [systemdrd.com/courses](https://systemdrd.com/courses/).

## Licence

MIT — see [LICENSE](LICENSE).
