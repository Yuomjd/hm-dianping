-- 1.参数列表
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]
-- 2.数据key
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
-- 3.脚本业务
    -- 1.判断库存
if( tonumber(redis.call('get',stockKey)) <= 0) then
    return 1
end
    -- 2.判断用户是否购买过
if(redis.call('sismember',orderKey,userId) == 1) then
    return 2
end
redis.call('incrby',stockKey,-1)
redis.call('sadd',orderKey,userId)
redis.call('XADD','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0