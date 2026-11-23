-- The delete of the Redis store (docs/interfaces.md I-26): the object, its key in the index, the
-- expiries and the ETags, and a notification of the key when it existed. KEYS: object, index,
-- fence, expiries, ETags. ARGV: key, channel, PUBLISH or SPUBLISH.
--
-- A missing key may be another delete not yet fsynced: the fence written here puts a write behind
-- it, so the WAITAOF that follows on this connection covers it too.
-- § dfd6vt90h.d@6pytj
if redis.call('DEL', KEYS[1]) == 1 then
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[4], ARGV[1])
  redis.call('HDEL', KEYS[5], ARGV[1])
  redis.call(ARGV[3], ARGV[2], ARGV[1])
else
  redis.call('INCR', KEYS[3])
end
return 1
-- § end dfd6vt90h.d
