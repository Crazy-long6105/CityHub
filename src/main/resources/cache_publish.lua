local ttl = tonumber(ARGV[3])
if not ttl or ttl < 0 then return redis.error_reply('invalid ttl') end
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
if ttl > 0 then
    redis.call('SET', KEYS[1], ARGV[2], 'PX', ttl)
else
    redis.call('SET', KEYS[1], ARGV[2])
end
redis.call('DEL', KEYS[2], KEYS[3])
return 1
