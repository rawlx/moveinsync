#!lua
-- Atomically assigns per-topic sequence numbers to one LOCATION event and stores it as the latest value for its
-- cab on every topic. Locations never enter the replay log (ADR-0002, ADR-0003).
--
-- 4 keys per topic i (0-based): KEYS[1+4i] seq, KEYS[2+4i] log, KEYS[3+4i] floor, KEYS[4+4i] loc
-- ARGV[1] event JSON   ARGV[2] cab id   ARGV[3] topic key TTL seconds
--
-- Reply: {'OK', publishedAtMs, seq_1..seq_n}. Loc hash values are "<seq>|<publishedAtMs>|<event JSON>".
-- Every check that can fail runs before the first write.

local function fmt(n)
  return string.format('%.0f', n)
end

local function numeric_or_absent(key)
  local t = redis.call('TYPE', key)['ok']
  return t == 'none' or (t == 'string' and tonumber(redis.call('GET', key)) ~= nil)
end

local function type_is(key, allowed)
  local t = redis.call('TYPE', key)['ok']
  return t == 'none' or t == allowed
end

local n = #KEYS / 4
for i = 0, n - 1 do
  if not numeric_or_absent(KEYS[1 + 4 * i]) or not numeric_or_absent(KEYS[3 + 4 * i]) then
    return redis.error_reply('TU_CORRUPT sequence state is not numeric')
  end
  if not type_is(KEYS[2 + 4 * i], 'stream') or not type_is(KEYS[4 + 4 * i], 'hash') then
    return redis.error_reply('TU_CORRUPT topic key has wrong type')
  end
end

local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local reply = {'OK', ms}
for i = 0, n - 1 do
  local seqKey, logKey, floorKey, locKey = KEYS[1 + 4 * i], KEYS[2 + 4 * i], KEYS[3 + 4 * i], KEYS[4 + 4 * i]
  if redis.call('EXISTS', seqKey) == 0 then
    local base = fmt(ms * 1000)
    redis.call('SET', seqKey, base)
    redis.call('SET', floorKey, base)
  end
  local seq = redis.call('INCR', seqKey)
  redis.call('HSET', locKey, ARGV[2], fmt(seq) .. '|' .. fmt(ms) .. '|' .. ARGV[1])
  for _, key in ipairs({seqKey, logKey, floorKey, locKey}) do
    redis.call('EXPIRE', key, ARGV[3])
  end
  reply[#reply + 1] = seq
end
return reply
