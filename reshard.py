"""Add a shard and move only the orders whose owner now maps elsewhere.

python3 reshard.py --add shard3          compare ring vs hash % N, then migrate
"""
import argparse
import json
import sqlite3

import shards
from ring import Ring, mod_node


def all_orders(names):
    rows = []
    for name in names:
        c = shards.connect(name)
        rows += [(name, dict(r)) for r in c.execute("SELECT * FROM orders")]
        c.close()
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--add", required=True)
    a = ap.parse_args()
    cfg = json.load(open(shards.CONFIG))
    old, new = cfg["shards"], cfg["shards"] + [a.add]
    rows = all_orders(old)
    users = {r["user_id"] for _, r in rows}
    ring_old, ring_new = Ring(old), Ring(new)
    moved_ring = [(src, r) for src, r in rows if ring_new.node_for(r["user_id"]) != src]
    moved_users_mod = sum(1 for u in users if mod_node(u, len(old)) != mod_node(u, len(new)))
    moved_users_ring = sum(1 for u in users if ring_old.node_for(u) != ring_new.node_for(u))
    print(f"{len(rows)} orders from {len(users)} users on {len(old)} shards; adding {a.add}")
    print(f"  hash % N would move {moved_users_mod / len(users):6.1%} of users")
    print(f"  the ring moves      {moved_users_ring / len(users):6.1%} of users ({len(moved_ring)} orders)")
    # copy first, switch the ring, then delete from the old shards
    for src, r in moved_ring:
        dst = shards.connect(ring_new.node_for(r["user_id"]))
        dst.execute("INSERT OR IGNORE INTO orders (id, user_id, product_id, qty, created_at) VALUES (?, ?, ?, ?, ?)",
                    (r["id"], r["user_id"], r["product_id"], r["qty"], r["created_at"]))
        dst.commit(); dst.close()
    cfg["shards"] = new
    json.dump(cfg, open(shards.CONFIG, "w"))
    for src, r in moved_ring:
        c = shards.connect(src)
        c.execute("DELETE FROM orders WHERE id = ?", (r["id"],)); c.commit(); c.close()
    after = all_orders(new)
    counts = {n: sum(1 for s, _ in after if s == n) for n in new}
    print(f"  after: {len(after)} orders, per shard {counts}")
