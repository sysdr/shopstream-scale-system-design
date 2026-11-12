"""Per-user token bucket in Redis.

Each user has a bucket of CAPACITY tokens that refills at RATE tokens per
second. An order costs one token; an empty bucket means HTTP 429. The check
and the update run as one Lua script, so two requests can't both take the
last token.

RATE_LIMIT=off turns it off (for the "before" measurement).
"""
import os
import time

from resp import client

ENABLED = os.environ.get("RATE_LIMIT", "on") != "off"
CAPACITY = int(os.environ.get("RL_CAPACITY", "5"))
RATE = float(os.environ.get("RL_RATE", "1"))  # tokens per second

TOKEN_BUCKET = """
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local bucket   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens   = tonumber(bucket[1]) or capacity
local ts       = tonumber(bucket[2]) or now
tokens = math.min(capacity, tokens + (now - ts) * rate)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)
local wait_ms = 0
if allowed == 0 then wait_ms = math.ceil((1 - tokens) / rate * 1000) end
return {allowed, wait_ms}
"""


def allow(user_id):
    """Returns (allowed, retry_after_seconds)."""
    if not ENABLED:
        return True, 0
    allowed, wait_ms = client().call(
        "EVAL", TOKEN_BUCKET, 1, f"rl:{user_id}", CAPACITY, RATE, f"{time.time():.3f}"
    )
    return allowed == 1, max(1, -(-wait_ms // 1000))
