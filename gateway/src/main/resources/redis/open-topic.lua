#!lua
-- Returns a topic's head sequence, first creating its sequence base if the topic does not exist yet (same
-- initialisation as the append scripts). A reader positioned at the returned head can then read every later event
-- without the topic's creation being mistaken for trimmed history.
--
-- KEYS[1] seq   KEYS[2] floor
-- ARGV[1] topic key TTL seconds
-- Reply: {head sequence}

local function fmt(n)
  return string.format('%.0f', n)
end

local function numeric_or_absent(key)
  local t = redis.call('TYPE', key)['ok']
  return t == 'none' or (t == 'string' and tonumber(redis.call('GET', key)) ~= nil)
end

if not numeric_or_absent(KEYS[1]) or not numeric_or_absent(KEYS[2]) then
  return redis.error_reply('TU_CORRUPT sequence state is not numeric')
end

local head = redis.call('GET', KEYS[1])
if head then
  return {tonumber(head)}
end

local now = redis.call('TIME')
local base = (tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)) * 1000
redis.call('SET', KEYS[1], fmt(base), 'EX', ARGV[1])
redis.call('SET', KEYS[2], fmt(base), 'EX', ARGV[1])
return {base}
