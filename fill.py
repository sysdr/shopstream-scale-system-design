"""Write a history of orders straight to the shards: USERS users x 10 orders."""
import random
import sys
import uuid

import shards

USERS = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
rng = random.Random(7)
for u in range(USERS):
    for _ in range(10):
        shards.insert_order(uuid.uuid4().hex[:12], f"user{u}", rng.randint(1, 2000), 1)
print(f"wrote {USERS * 10} orders")
