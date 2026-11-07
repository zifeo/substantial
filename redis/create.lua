-- The create of the Redis store (docs/interfaces.md I-26): the object if it is missing, its key in
-- the index, a notification of the key. KEYS: object, index, fence. ARGV: key, bytes, channel,
-- PUBLISH or SPUBLISH. Returns 1 when created, 0 when the key exists.
--
-- An existing key may be another writer's create not yet fsynced: the fence written here puts a
-- write behind it, so the WAITAOF that follows on this connection covers it too.
if redis.call('SET', KEYS[1], ARGV[2], 'NX') then
  redis.call('ZADD', KEYS[2], 0, ARGV[1])
  redis.call(ARGV[4], ARGV[3], ARGV[1])
  return 1
end
redis.call('INCR', KEYS[3])
return 0
