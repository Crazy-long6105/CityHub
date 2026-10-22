local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local member = ARGV[3]
if #KEYS < 1 or #KEYS > 3 or #ARGV ~= 3
    or not window or window < 1 or window > 86400 or window ~= math.floor(window)
    or not limit or limit < 1 or limit > 1000000 or limit ~= math.floor(limit)
    or not member or #member ~= 36 or not string.match(member, '^[%x%-]+$') then
    return redis.error_reply('Invalid rate limiter parameters')
end

-- Redis does not roll back script errors. Validate every key before any mutation.
local seen = {}
for i, key in ipairs(KEYS) do
    if #key == 0 or #key > 512 or seen[key] then
        return redis.error_reply('Invalid or duplicate limiter key')
    end
    seen[key] = true
    local kind = redis.call('TYPE', key).ok
    if kind ~= 'none' and kind ~= 'zset' then
        return redis.error_reply('Invalid limiter key type')
    end
    if kind == 'zset' and redis.call('ZSCORE', key, member) then
        return redis.error_reply('Duplicate limiter request identifier')
    end
end

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local counts = {}
local rejected = 0
for i, key in ipairs(KEYS) do
    redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window * 1000)
    counts[i] = redis.call('ZCARD', key)
    if counts[i] >= limit and rejected == 0 then
        rejected = i
    end
end
local result = {0, rejected}
if rejected == 0 then
    result[1] = 1
    for i, key in ipairs(KEYS) do
        redis.call('ZADD', key, now, member)
        redis.call('PEXPIRE', key, window * 1000)
        counts[i] = counts[i] + 1
    end
end
for i, count in ipairs(counts) do
    result[#result + 1] = count
end
return result
