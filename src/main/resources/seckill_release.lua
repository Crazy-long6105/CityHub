-- Completion markers remain until their durable owner confirms completion.
local function kind(key) return redis.call('TYPE', key).ok end
if #KEYS ~= 3 or #ARGV ~= 2 then return redis.error_reply('invalid release') end
if kind(KEYS[3]) == 'string' and redis.call('GET', KEYS[3]) == ARGV[2] then return 1 end
if (kind(KEYS[3]) ~= 'none' and kind(KEYS[3]) ~= 'string')
    or kind(KEYS[1]) ~= 'string' or kind(KEYS[2]) ~= 'hash' then return redis.error_reply('missing control data') end
local raw = redis.call('GET', KEYS[1])
if not string.match(raw, '^%d+$') or tonumber(raw) >= 2147483647 then return redis.error_reply('invalid control stock') end
local current = redis.call('HGET', KEYS[2], ARGV[1])
if not current then return redis.error_reply('qualification missing without release evidence') end
if current ~= ARGV[2] then
    redis.call('SET', KEYS[3], ARGV[2])
    return 2
end
redis.call('INCR', KEYS[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('SET', KEYS[3], ARGV[2])
return 0
