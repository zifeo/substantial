-- The compare-and-set of the Redis store (docs/interfaces.md I-26): the object replaced only if
-- its value's SHA-1 is the ETag named, its new ETag in the ETags, its expiry set anew, a
-- notification of the key. KEYS: object, index, fence, expiries, ETags. ARGV: key, bytes, ETag,
-- the expiry in ms (0: none), channel, PUBLISH or SPUBLISH. Returns 1 when written, 0 on a
-- mismatch, a missing key included.
--
-- A mismatch may be another writer's write not yet fsynced: the fence written here puts a write
-- behind it, so the WAITAOF that follows on this connection covers it too.
-- § rz3se15bd@13bx6
local cur = redis.call('GET', KEYS[1])
if not cur or redis.sha1hex(cur) ~= ARGV[3] then
  redis.call('INCR', KEYS[3])
  return 0
end
-- § end rz3se15bd
-- § hxvcx7f2a@3mfbc
local ms = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
-- A plain SET clears the object's TTL, as a write without an expiry must.
if ms == 0 then
  redis.call('SET', KEYS[1], ARGV[2])
  redis.call('ZREM', KEYS[4], ARGV[1])
else
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ms)
  redis.call('ZADD', KEYS[4], now + ms, ARGV[1])
end
redis.call('ZADD', KEYS[2], 0, ARGV[1])
redis.call('HSET', KEYS[5], ARGV[1], redis.sha1hex(ARGV[2]))
for _, k in ipairs(redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, 16)) do
  redis.call('ZREM', KEYS[2], k)
  redis.call('ZREM', KEYS[4], k)
  redis.call('HDEL', KEYS[5], k)
end
-- § end hxvcx7f2a
redis.call(ARGV[6], ARGV[5], ARGV[1])
return 1
