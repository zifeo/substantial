-- The delete of the Redis store (docs/interfaces.md I-26): the object, its key in the index, and
-- a notification of the key when it existed. KEYS: object, index, fence. ARGV: key, channel,
-- PUBLISH or SPUBLISH.
--
-- A missing key may be another delete not yet fsynced: the fence written here puts a write behind
-- it, so the WAITAOF that follows on this connection covers it too.
if redis.call('DEL', KEYS[1]) == 1 then
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call(ARGV[3], ARGV[2], ARGV[1])
else
  redis.call('INCR', KEYS[3])
end
return 1
