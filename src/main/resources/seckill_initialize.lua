if redis.call('EXISTS', KEYS[1], KEYS[2], KEYS[3]) ~= 0 then return redis.error_reply('activity already initialized') end
local stock, beginAt, endAt = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
if not stock or stock < 0 or stock > 2147483647 or stock ~= math.floor(stock)
    or not beginAt or not endAt or beginAt >= endAt then return redis.error_reply('invalid activity') end
redis.call('HSET', KEYS[1], 'begin', ARGV[2], 'end', ARGV[3])
redis.call('SET', KEYS[2], ARGV[1])
return 1
