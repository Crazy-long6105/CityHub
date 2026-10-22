local ttl = tonumber(ARGV[2])
if not ttl or ttl <= 0 then return redis.error_reply('invalid ttl') end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], '1', 'PX', ttl)
redis.call('DEL', KEYS[1])
return 1
