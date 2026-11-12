"""Ship each shard leader's changelog to its follower, every INTERVAL seconds.

Real databases stream their write-ahead log continuously; polling makes the
lag easy to see. Lag = time from the leader's commit to the follower's apply.
"""
import json
import os
import statistics
import time

import shards

INTERVAL = float(os.environ.get("REPLICA_INTERVAL", "0.5"))


def apply_batch(shard, lags):
    leader, follower = shards.connect(shard), shards.connect_follower(shard)
    done = follower.execute("SELECT applied_lsn FROM replica_state").fetchone()[0]
    rows = leader.execute("SELECT lsn, ts, op, row FROM changelog WHERE lsn > ? ORDER BY lsn LIMIT 5000", (done,)).fetchall()
    if rows:
        with follower:
            for r in rows:
                o = json.loads(r["row"])
                follower.execute("INSERT OR IGNORE INTO orders (id, user_id, product_id, qty) VALUES (:id, :user_id, :product_id, :qty)", o)
            follower.execute("UPDATE replica_state SET applied_lsn = ?", (rows[-1]["lsn"],))
        now = time.time()
        lags += [(now - r["ts"]) * 1000 for r in rows]
    leader.close(); follower.close()
    return len(rows)


if __name__ == "__main__":
    lags, last_report = [], time.time()
    print(f"replicating every {INTERVAL}s", flush=True)
    while True:
        for s in shards.load_ring().nodes:
            apply_batch(s, lags)
        if lags and time.time() - last_report > 5:
            lags.sort()
            print(f"applied {len(lags)} writes; lag p50 {statistics.median(lags):.0f} ms, p99 {lags[int(len(lags)*0.99)]:.0f} ms", flush=True)
            with open("replica_lag.json", "w") as f:
                json.dump({"n": len(lags), "p50": statistics.median(lags), "p99": lags[int(len(lags)*0.99)]}, f)
            lags, last_report = [], time.time()
        time.sleep(INTERVAL)
