local status = redis.call('HGET', KEYS[1], 'status')
if not status then return redis.error_reply('request missing') end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if ARGV[1] == 'FINISH' then
    if status == 'SUCCESS' or status == 'FAILED' then return 0 end
    redis.call('HSET', KEYS[1], 'status', ARGV[2], 'reason', ARGV[3])
    redis.call('ZREM', KEYS[2], ARGV[4])
    local credential = tonumber(redis.call('HGET', KEYS[1], 'credentialExpiry')) or now
    redis.call('PEXPIREAT', KEYS[1], math.max(now + 86400000, credential))
elseif ARGV[1] == 'SEND' then
    if status == 'SUCCESS' or status == 'FAILED' then return 0 end
    if now >= tonumber(redis.call('HGET', KEYS[1], 'deadline')) then return 0 end
    local attempts = tonumber(redis.call('HGET', KEYS[1], 'attempts')) or 0
    local nextSend = tonumber(redis.call('HGET', KEYS[1], 'nextSend')) or 0
    if attempts >= 3 or now < nextSend or redis.call('HGET', KEYS[1], 'sendState') == 'ACK' then return 0 end
    redis.call('HSET', KEYS[1], 'attempts', attempts + 1, 'nextSend', now + 5000, 'sendState', 'UNKNOWN')
    return 1
elseif ARGV[1] == 'ACK' then
    redis.call('HSET', KEYS[1], 'sendState', 'ACK')
elseif ARGV[1] == 'WAIT' then
    if status ~= 'SUCCESS' and status ~= 'FAILED' then
        redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[4])
    end
end
return 1
