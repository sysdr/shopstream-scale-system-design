"""Consistent hashing with virtual nodes.

Each database owns many points ("virtual nodes") on a ring of 2**64 hash
values. A key belongs to the first point clockwise from its own hash.
Adding a database only takes over the arcs next to its new points, so only
about 1/N of the keys move.
"""
import bisect
import hashlib


def h(key: str) -> int:
    return int.from_bytes(hashlib.md5(key.encode()).digest()[:8], "big")


class Ring:
    def __init__(self, nodes, vnodes=100):
        self.vnodes = vnodes
        self._points, self._owner = [], {}
        for n in nodes:
            self.add(n)

    def add(self, node):
        for i in range(self.vnodes):
            p = h(f"{node}#{i}")
            bisect.insort(self._points, p)
            self._owner[p] = node

    @property
    def nodes(self):
        return sorted(set(self._owner.values()))

    def node_for(self, key):
        i = bisect.bisect(self._points, h(key)) % len(self._points)
        return self._owner[self._points[i]]


def mod_node(key, n):
    """The naive alternative: hash(key) % N."""
    return f"shard{h(key) % n}"


if __name__ == "__main__":
    keys = [f"order:{i}" for i in range(100_000)]
    before = {k: mod_node(k, 3) for k in keys}
    after = {k: mod_node(k, 4) for k in keys}
    moved_mod = sum(before[k] != after[k] for k in keys)
    for v in (1, 10, 100, 200):
        r3 = Ring(["db0", "db1", "db2"], vnodes=v)
        a3 = {k: r3.node_for(k) for k in keys}
        r4 = Ring(["db0", "db1", "db2"], vnodes=v); r4.add("db3")
        a4 = {k: r4.node_for(k) for k in keys}
        moved = sum(a3[k] != a4[k] for k in keys)
        load = [sum(1 for k in keys if a4[k] == f"db{i}") for i in range(4)]
        spread = (max(load) - min(load)) / (len(keys) / 4) * 100
        print(f"vnodes={v:>3}: moved {moved/len(keys)*100:5.1f}% | 4-node load {load} | max-min spread {spread:4.1f}% of fair share")
    print(f"hash % N (3 -> 4): moved {moved_mod/len(keys)*100:5.1f}%")
