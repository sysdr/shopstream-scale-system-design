"""ShopStream storage: one SQLite file, seeded with products and reviews."""
import os
import random
import sqlite3

DB_PATH = os.environ.get("SHOP_DB", "shop.db")


def connect(path=DB_PATH):
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def seed(path=DB_PATH, products=2_000, reviews=200_000):
    """Create tables and fill them once. Reviews have no index on product_id
    on purpose: the product page has to scan them, like a slow real query."""
    conn = connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL, price_cents INTEGER NOT NULL, stock INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY, product_id INTEGER NOT NULL, stars INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, product_id INTEGER NOT NULL,
            qty INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
        """
    )
    if conn.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 0:
        rng = random.Random(42)
        conn.executemany(
            "INSERT INTO products (id, name, price_cents, stock) VALUES (?, ?, ?, ?)",
            [(i, f"Product {i}", rng.randint(199, 99_999), 1_000) for i in range(1, products + 1)],
        )
        conn.executemany(
            "INSERT INTO reviews (product_id, stars) VALUES (?, ?)",
            [(rng.randint(1, products), rng.randint(1, 5)) for _ in range(reviews)],
        )
        conn.commit()
    conn.close()


def product_page(conn, product_id):
    """The expensive read: product row plus its rating, computed from reviews."""
    row = conn.execute(
        "SELECT id, name, price_cents, stock FROM products WHERE id = ?", (product_id,)
    ).fetchone()
    if row is None:
        return None
    stats = conn.execute(
        "SELECT COUNT(*) AS n, ROUND(AVG(stars), 2) AS avg FROM reviews WHERE product_id = ?",
        (product_id,),
    ).fetchone()
    return {**dict(row), "reviews": stats["n"], "rating": stats["avg"]}


def place_order(conn, user_id, product_id, qty):
    """Take stock in the catalog database, then write the order to its shard.

    These are two databases, so this is not one transaction: if the shard
    write fails we put the stock back (a compensating action)."""
    import uuid

    import shards

    cur = conn.execute(
        "UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?", (qty, product_id, qty)
    )
    if cur.rowcount == 0:
        conn.rollback()
        return None
    conn.commit()
    order_id = uuid.uuid4().hex[:12]
    try:
        shard, lsn = shards.insert_order(order_id, user_id, product_id, qty)
    except Exception:
        conn.execute("UPDATE products SET stock = stock + ? WHERE id = ?", (qty, product_id))
        conn.commit()
        raise
    return {"order_id": order_id, "shard": shard, "lsn": lsn}


if __name__ == "__main__":
    seed()
    print("seeded", DB_PATH)
