#!/usr/bin/env bash
# ShopStream — stop everything start.sh started (app, replica, worker, Redis on 6390).
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT/shopstream" 2>/dev/null || { echo "Nothing to stop."; exit 0; }
for f in .run/app.pid .run/replica.pid .run/worker.pid; do
  [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null && echo "stopped $(basename "$f" .pid)"
  rm -f "$f"
done
pkill -f "python3 (app|replica|worker).py" 2>/dev/null || true
if [ -f .run/redis.pid ]; then
  redis-cli -p 6390 shutdown nosave >/dev/null 2>&1 && echo "stopped redis (port 6390)"
  rm -f .run/redis.pid
fi
echo "Done. Your code is still in ./shopstream."
