#!lua
-- Atomically assigns per-topic sequence numbers to one ordered event (STATUS, DELAY, SOS), appends it to every
-- topic log and to the critical bus, and records the result for idempotent retries (ADR-0001, ADR-0003).
--
-- KEYS[1] dedup key   KEYS[2] bus stream
-- then 4 keys per topic i (0-based): KEYS[3+4i] seq, KEYS[4+4i] log, KEYS[5+4i] floor, KEYS[6+4i] loc
-- ARGV[1] tenant id   ARGV[2] event JSON   ARGV[3] log max length   ARGV[4] bus max length
-- ARGV[5] topic key TTL seconds   ARGV[6] dedup TTL seconds   ARGV[7..] topic names (same order as the keys)
--
-- Reply: {'OK', publishedAtMs, seq_1..seq_n} | {'DUP', publishedAtMs, seq_1..seq_n} | {'CONFLICT'}
-- Every check that can fail runs before the first write, so an error reply never leaves a partial append.

local function fmt(n)
  return string.format('%.0f', n)
end

local function seq_of(id)
  return tonumber(string.sub(id, 1, string.find(id, '-', 1, true) - 1))
end

local function numeric_or_absent(key)
  local t = redis.call('TYPE', key)['ok']
  return t == 'none' or (t == 'string' and tonumber(redis.call('GET', key)) ~= nil)
end

local function type_is(key, allowed)
  local t = redis.call('TYPE', key)['ok']
  return t == 'none' or t == allowed
end

local n = (#KEYS - 2) / 4
local body = ARGV[2]
local fingerprint = redis.sha1hex(body .. '\n' .. table.concat(ARGV, ',', 7))

-- 1. Idempotent retry: same event id seen within the dedup window.
if not type_is(KEYS[1], 'string') then
  return redis.error_reply('TU_CORRUPT dedup key has wrong type')
end
local prior = redis.call('GET', KEYS[1])
if prior then
  local sep = string.find(prior, '|', 1, true)
  if string.sub(prior, 1, sep - 1) ~= fingerprint then
    return {'CONFLICT'}
  end
  local reply = {'DUP'}
  for v in string.gmatch(string.sub(prior, sep + 1), '[^,]+') do
    reply[#reply + 1] = tonumber(v)
  end
  return reply
end

-- 2. Pre-checks: nothing below may fail once writing starts.
if not type_is(KEYS[2], 'stream') then
  return redis.error_reply('TU_CORRUPT bus key has wrong type')
end
for i = 0, n - 1 do
  local seqKey, logKey, floorKey, locKey = KEYS[3 + 4 * i], KEYS[4 + 4 * i], KEYS[5 + 4 * i], KEYS[6 + 4 * i]
  if not numeric_or_absent(seqKey) or not numeric_or_absent(floorKey) then
    return redis.error_reply('TU_CORRUPT sequence state is not numeric')
  end
  if not type_is(logKey, 'stream') or not type_is(locKey, 'hash') then
    return redis.error_reply('TU_CORRUPT topic key has wrong type')
  end
  local last = redis.call('XREVRANGE', logKey, '+', '-', 'COUNT', 1)
  if #last > 0 and seq_of(last[1][1]) > tonumber(redis.call('GET', seqKey) or '0') then
    return redis.error_reply('TU_CORRUPT log is ahead of its sequence counter')
  end
end

-- 3. Writes.
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local logMax, busMax = tonumber(ARGV[3]), ARGV[4]
local ttl = ARGV[5]
local seqs, topicSeqs = {}, {}
for i = 0, n - 1 do
  local seqKey, logKey, floorKey, locKey = KEYS[3 + 4 * i], KEYS[4 + 4 * i], KEYS[5 + 4 * i], KEYS[6 + 4 * i]
  if redis.call('EXISTS', seqKey) == 0 then
    -- New (or expired) topic: start above every sequence it can ever have used, and mark everything up to the
    -- base as unavailable history, so an old resume position is reported as a gap instead of being reused.
    local base = fmt(ms * 1000)
    redis.call('SET', seqKey, base)
    redis.call('SET', floorKey, base)
  end
  local seq = redis.call('INCR', seqKey)
  redis.call('XADD', logKey, fmt(seq) .. '-0', 'event', body, 'publishedAt', fmt(ms))
  local len = redis.call('XLEN', logKey)
  if len > logMax then
    local trimmed = redis.call('XRANGE', logKey, '-', '+', 'COUNT', len - logMax)
    local floor = seq_of(trimmed[#trimmed][1])
    redis.call('SET', floorKey, fmt(floor))
    redis.call('XTRIM', logKey, 'MINID', fmt(floor + 1))
  end
  for _, key in ipairs({seqKey, logKey, floorKey, locKey}) do
    redis.call('EXPIRE', key, ttl)
  end
  seqs[i + 1] = seq
  topicSeqs[i + 1] = ARGV[7 + i] .. '=' .. fmt(seq)
end

redis.call('XADD', KEYS[2], 'MAXLEN', '~', busMax, '*',
  'tenant', ARGV[1], 'event', body, 'publishedAt', fmt(ms), 'seqs', table.concat(topicSeqs, ','))

local stored = {}
for i = 1, n do
  stored[i] = fmt(seqs[i])
end
redis.call('SET', KEYS[1], fingerprint .. '|' .. fmt(ms) .. ',' .. table.concat(stored, ','), 'EX', ARGV[6])

local reply = {'OK', ms}
for i = 1, n do
  reply[#reply + 1] = seqs[i]
end
return reply
