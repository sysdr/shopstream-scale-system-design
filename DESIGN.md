# ShopStream — one-page design

## Requirements I designed for
- Product pages: read-heavy (100:1 reads to writes), p99 under 100 ms at peak.
- Checkout: never oversell; one bot must not starve real shoppers.
- Orders: grow past one database without a big-bang migration.
- "My orders" must show an order the moment it is placed.
- A slow or failing email provider must not slow checkout or lose receipts.

## Architecture
```
client ──> app.py (stateless HTTP, many threads; add instances behind a load balancer)
             │ GET /products/<id>  ──> Redis cache-aside (TTL 30 s ± jitter, rebuild lock) ──miss──> SQLite products/reviews
             │ POST /orders        ──> Redis token bucket (5 burst, 1/s per user) ──> stock decrement (conditional UPDATE)
             │                         ──> orders shard leader (consistent-hash ring, 100 vnodes) + changelog
             │                         ──> XADD receipts ──> worker.py (consumer group, retries, DLQ)
             │ GET /users/<id>/orders ──> shard follower; leader if follower LSN < client's last write LSN
replica.py ── ships each leader's changelog to its follower every 0.5 s
```

## Numbers I measured (2-core laptop, lessons 1–6)
| Change | Before | After |
|---|---|---|
| Cache on product pages (50 users) | 280 req/s, p99 514 ms | 5,256 req/s, p99 29 ms |
| Herd of 50 on one expired key | 9–11 DB reads | 1 DB read |
| Bot vs 5 shoppers, product with 1,000 units | shoppers got 5 of 25 tries | shoppers got 25 of 25; bot got 15 |
| Add a 4th shard (2,000 users) | hash % N moves 74% of users | ring moves 22% |
| "My orders" right after checkout | 100/100 stale from follower | 0/100 stale with read-your-own-writes |
| Checkout with a 300 ms email provider | p50 302 ms | p50 0.9 ms (queued) |

## Trade-offs I chose, and when I'd change them
- **Cache TTL 30 s**: prices/stock on the page can be 30 s old; checkout re-checks stock in the database, so no oversell. For flash sales, invalidate on write instead.
- **Token bucket per user**: simple, allows short bursts. A bot with many accounts needs per-IP/device limits too.
- **Shard by user_id**: "my orders" hits one shard. "All orders for product X" now fans out to every shard — keep a separate analytics copy for that.
- **Async replication**: fast writes, but a follower can lag (p50 263 ms, p99 503 ms here) and a leader crash can lose the last half-second. Sync replication would cost a network round-trip per write.
- **At-least-once queue + idempotency key**: a receipt is never lost; the dedupe key stops double emails after a crash. Exactly-once delivery to an outside provider isn't possible; idempotency is how we fake it.
- **Saga, not a distributed transaction**: stock and order live in different databases; a failed order write adds the stock back.

## What I'd do next
Move products to Postgres with a read replica; put two app instances behind a load balancer; Redis Sentinel or a managed Redis for failover; Kafka instead of Redis Streams when several services need the order events (next course).
