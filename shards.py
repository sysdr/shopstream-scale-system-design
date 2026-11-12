"""Orders live in N SQLite shards, chosen by user_id on a consistent-hash ring.

Lesson 5: every shard is a leader with a follower copy. Writes go to the
leader and append to its changelog in the same transaction; replica.py
ships the changelog to the follower. Reads go to the follower, unless the
caller has just written something the follower hasn't applied yet
(read-your-own-writes).

shards.json lists the shards ({"shards": ["shard0", "shard1", "shard2"]}).
All of a user's orders sit on one shard, so "my orders" reads one database.
"""
import json
import os
import sqlite3
import threading

from ring import Ring

CONFIG = os.environ.get("SHARDS_FILE", "shards.json")
_local = threading.local()


def load_ring():
    if not os.path.exists(CONFIG):
        with open(CONFIG, "w") as f:
            json.dump({"shards": ["shard0", "shard1", "shard2"]}, f)
    with open(CONFIG) as f:
        return Ring(json.load(f)["shards"])


def connect(shard):
    conn = sqlite3.connect(f"orders-{shard}.db", timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY, user_id TEXT NOT NULL, product_id INTEGER NOT NULL,
            qty INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS orders_user ON orders(user_id)")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS changelog (
            lsn INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, op TEXT NOT NULL, row TEXT NOT NULL)"""
    )
    conn.execute("CREATE TABLE IF NOT EXISTS replica_state (id INTEGER PRIMARY KEY CHECK (id = 1), applied_lsn INTEGER NOT NULL)")
    conn.execute("INSERT OR IGNORE INTO replica_state (id, applied_lsn) VALUES (1, 0)")
    conn.commit()
    return conn


def connect_follower(shard):
    return connect(f"{shard}-follower")


def shard_conn(shard, follower=False):
    cache = getattr(_local, "conns", None)
    if cache is None:
        cache = _local.conns = {}
    key = (shard, follower)
    if key not in cache:
        cache[key] = connect_follower(shard) if follower else connect(shard)
    return cache[key]


READ_MODE = os.environ.get("READ_MODE", "ryw")  # "follower" = always read the follower
stats = {"follower_reads": 0, "leader_reads": 0}


RING = load_ring()


def insert_order(order_id, user_id, product_id, qty):
    """Write to the shard leader. Returns (shard, lsn) — the lsn is the
    position of this write in the leader's changelog."""
    import time

    shard = RING.node_for(user_id)
    c = shard_conn(shard)
    row = {"id": order_id, "user_id": user_id, "product_id": product_id, "qty": qty}
    with c:  # one transaction: the order and its changelog entry
        c.execute("INSERT INTO orders (id, user_id, product_id, qty) VALUES (:id, :user_id, :product_id, :qty)", row)
        lsn = c.execute("INSERT INTO changelog (ts, op, row) VALUES (?, 'insert', ?)",
                        (time.time(), json.dumps(row))).lastrowid
    return shard, lsn


def follower_lsn(shard):
    return shard_conn(shard, follower=True).execute("SELECT applied_lsn FROM replica_state").fetchone()[0]


def orders_for(user_id, min_lsn=0):
    """Read from the follower; if the caller needs a write the follower
    hasn't applied yet (min_lsn), read the leader instead."""
    shard = RING.node_for(user_id)
    use_leader = READ_MODE == "ryw" and min_lsn and follower_lsn(shard) < min_lsn
    stats["leader_reads" if use_leader else "follower_reads"] += 1
    rows = shard_conn(shard, follower=not use_leader).execute(
        "SELECT id, product_id, qty, created_at FROM orders WHERE user_id = ? ORDER BY created_at", (user_id,)
    ).fetchall()
    return shard, [dict(r) for r in rows], ("leader" if use_leader else "follower")
