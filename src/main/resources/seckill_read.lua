local t = redis.call('TIME')
local result = {tostring(tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000))}
local fields = redis.call('HGETALL', KEYS[1])
for _, value in ipairs(fields) do table.insert(result, value) end
return result
