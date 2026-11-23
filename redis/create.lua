-- The create of the Redis store (docs/interfaces.md I-26): the object if it is missing, its key in
-- the index, its ETag in the ETags, its expiry, a notification of the key. KEYS: object, index,
-- fence, expiries, ETags. ARGV: key, bytes, channel, PUBLISH or SPUBLISH, the expiry in ms (0:
-- none). Returns 1 when created, 0 when the key exists.
--
-- An existing key may be another writer's create not yet fsynced: the fence written here puts a
-- write behind it, so the WAITAOF that follows on this connection covers it too.
-- § dfd6vt90h@m02sq
local ms = tonumber(ARGV[5])
local created
if ms == 0 then
  created = redis.call('SET', KEYS[1], ARGV[2], 'NX')
else
  created = redis.call('SET', KEYS[1], ARGV[2], 'NX', 'PX', ms)
end
if created then
  redis.call('ZADD', KEYS[2], 0, ARGV[1])
  -- § bdh1dmwjr.a@3y730
  redis.call('HSET', KEYS[5], ARGV[1], redis.sha1hex(ARGV[2]))
  -- § end bdh1dmwjr.a
  -- § hxvcx7f2a@d7ngj
  local t = redis.call('TIME')
  local now = t[1] * 1000 + math.floor(t[2] / 1000)
  if ms == 0 then
    redis.call('ZREM', KEYS[4], ARGV[1])
  else
    redis.call('ZADD', KEYS[4], now + ms, ARGV[1])
  end
  -- Redis expires an object, not its index member: members of objects expired go, 16 at a time.
  for _, k in ipairs(redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', now, 'LIMIT', 0, 16)) do
    redis.call('ZREM', KEYS[2], k)
    redis.call('ZREM', KEYS[4], k)
    redis.call('HDEL', KEYS[5], k)
  end
  -- § end hxvcx7f2a
  redis.call(ARGV[4], ARGV[3], ARGV[1])
  return 1
end
-- § end dfd6vt90h
redis.call('INCR', KEYS[3])
return 0
