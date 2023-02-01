package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public <T> void setWithLogicalExpire(String key, T value, Long time, TimeUnit unit){
//        设置逻辑过期
        RedisData<T> redisData = new RedisData<>();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
//        写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 设置空值防止缓存穿透
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param timeUnit
     * @param timeInBlank
     * @param timeUnitInBlank
     * @return
     * @param <T>
     * @param <ID>
     */
    public <T,ID> T queryWithPassThrough(String keyPrefix, ID id, Class<T> type, Function<ID,T> dbFallback,Long time,
                                         TimeUnit timeUnit,Long timeInBlank,TimeUnit timeUnitInBlank){
        String key = keyPrefix + id;
        String json = JSONUtil.toJsonStr(stringRedisTemplate.opsForValue().get(key));
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json,type);
        }
        if(json != null){
            return null;
        }
        T t = dbFallback.apply(id);
        if(t == null){
            stringRedisTemplate.opsForValue().set(key,"",timeInBlank,timeUnitInBlank);
            return null;
        }
        this.set(key,t,time,timeUnit);
        return t;
    }

    /**
     * 逻辑删除解决缓存击穿
     * @param id
     * @return
     */
    public <T,ID> T queryWithLogicalExpire(String keyPrefix,ID id,Class<T> type,Function<ID,T> dbFallBack,Long time,
                                           TimeUnit timeUnit){
//        1.从redis里查询缓存
        String s = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(s);
//        2.判断是否存在
        if(StrUtil.isBlank(json)){
//        3.不存在，返回null
            return null;
        }
//        4.命中，需要将json反序列化为对象
        RedisData<T> redisData = JSONUtil.toBean(json, RedisData.class);
        T t = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
//        5.判断是否过期，
        if(expireTime.isAfter(LocalDateTime.now())){
//          5.1未过期，返回
            return t;
        }
//        5.2过期，重建缓存
//        6.1获取互斥锁
        String key = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(key);
//        6.2判断是否获取成功
        if(isLock){
//          6.3成功获取，开启独立线程，将缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
//                    查询数据库
                    T t1 = dbFallBack.apply(id);
//                    存入缓存
                    this.setWithLogicalExpire(s,t1,time,timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
//                  释放锁
                    unLock(key);
                }
            });
        }
//        7.返回旧数据
        return t;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
