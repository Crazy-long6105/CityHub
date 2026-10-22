-- KEYS: activity, control stock, qualifications, request, recovery index.
-- ARGV: voucher, user, order, first-reservation permit deadline, credential expiry.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local function kind(key) return redis.call('TYPE', key).ok end
if #KEYS ~= 5 or #ARGV ~= 5 then return {'INVALID', ''} end
for i = 1, 3 do if not string.match(ARGV[i], '^%d+$') then return {'INVALID', ''} end end
local permit, credential = tonumber(ARGV[4]), tonumber(ARGV[5])
if not permit or not credential or credential < permit then return {'INVALID', ''} end
if kind(KEYS[1]) ~= 'hash' or kind(KEYS[2]) ~= 'string'
    or (kind(KEYS[3]) ~= 'none' and kind(KEYS[3]) ~= 'hash')
    or (kind(KEYS[4]) ~= 'none' and kind(KEYS[4]) ~= 'hash')
    or (kind(KEYS[5]) ~= 'none' and kind(KEYS[5]) ~= 'zset') then return {'UNAVAILABLE', ''} end
if redis.call('EXISTS', KEYS[4]) == 1 then
    if redis.call('HGET', KEYS[4], 'userId') ~= ARGV[2]
        or redis.call('HGET', KEYS[4], 'voucherId') ~= ARGV[1] then return {'INVALID', ''} end
    return {'ACCEPTED', ARGV[3]}
end
if now > permit then return {'NOT_ACCEPTED', ARGV[3]} end
local existing = redis.call('HGET', KEYS[3], ARGV[2])
if existing then return {'EXISTING', existing} end
local beginAt = tonumber(redis.call('HGET', KEYS[1], 'begin'))
local endAt = tonumber(redis.call('HGET', KEYS[1], 'end'))
local raw = redis.call('GET', KEYS[2])
if not beginAt or not endAt or not string.match(raw, '^%d+$') then return {'UNAVAILABLE', ''} end
local stock = tonumber(raw)
if not stock or stock > 2147483647 then return {'UNAVAILABLE', ''} end
if now < beginAt then return {'NOT_STARTED', ''} end
if now >= endAt then return {'ENDED', ''} end
if stock <= 0 then return {'SOLD_OUT', ''} end
redis.call('DECR', KEYS[2])
redis.call('HSET', KEYS[3], ARGV[2], ARGV[3])
redis.call('HSET', KEYS[4], 'orderId', ARGV[3], 'userId', ARGV[2], 'voucherId', ARGV[1],
    'createdAt', now, 'deadline', now + 30000, 'credentialExpiry', credential,
    'status', 'PROCESSING', 'sendState', 'PENDING', 'attempts', 0)
redis.call('ZADD', KEYS[5], now + 1000, ARGV[3])
return {'ACCEPTED', ARGV[3]}
