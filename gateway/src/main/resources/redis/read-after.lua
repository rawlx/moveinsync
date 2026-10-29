#!lua flags=no-writes
-- Atomically reads a topic's head sequence, its floor (highest sequence no longer in the log) and, when the
-- requested range is still complete, the ordered events after ARGV[1].
--
-- KEYS[1] seq   KEYS[2] floor   KEYS[3] log
-- ARGV[1] after-seq (exclusive)   ARGV[2] max entries
-- Reply: {head, floor, entries}; entries is empty when after-seq < floor or after-seq > head (history gap).

local head = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(redis.call('GET', KEYS[2]) or '0')
local after = tonumber(ARGV[1])
local entries = {}
if after >= floor and after <= head then
  entries = redis.call('XRANGE', KEYS[3], '(' .. ARGV[1], '+', 'COUNT', ARGV[2])
end
return {head, floor, entries}
