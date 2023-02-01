package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
//        互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
//        逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,20L,
                TimeUnit.SECONDS);
        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    /*
      互斥锁解决缓存击穿
      @param id
     * @return
     */
//    public Shop queryWithMutex(Long id) {
//        //        1.从redis里查询缓存
//        String s = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(s);
////        2.判断是否存在
//        if(StrUtil.isNotBlank(shopJson)){
////            存在，返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
////        3.判断命中的是否是空值
//        if(shopJson != null){
//            return null;
//        }
////      4.实现缓存重建
////        4.1获取互斥锁
//        String key = LOCK_SHOP_KEY + id;
//        boolean b = tryLock(key);
////        4.2判断是否获取成功
//        Shop shop = null;
//        try {
//            if(!b){
////              4.3如果失败，则休眠并重试
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
////        4.4如果成功，则再次检查缓存中是否存在
//            shopJson = stringRedisTemplate.opsForValue().get(s);
//            if(StrUtil.isNotBlank(shopJson)){
////            存在，返回
//                shop = JSONUtil.toBean(shopJson, Shop.class);
//                return shop;
//            }
////        3.判断命中的是否是空值
//            if(shopJson != null){
//                return null;
//            }
////        5.不存在，查询数据库
//            shop = getById(id);
//            Thread.sleep(200);
//            if (shop == null){
////            不存在，将空值写入缓存
//                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
////            返回错误信息
//                return null;
//            }
////        6.存在，写入redis
//            stringRedisTemplate.opsForValue().set(s,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
////        释放互斥锁
//            unLock(key);
//        }
////        7.返回
//        return shop;
//    }


    /**
     * 逻辑删除解决缓存击穿
     */
//    public Shop queryWithLogicalExpire(Long id){
////        1.从redis里查询缓存
//        String s = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(s);
////        2.判断是否存在
//        if(StrUtil.isBlank(shopJson)){
////        3.不存在，返回null
//            return null;
//        }
////        4.命中，需要将json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
////        5.判断是否过期，
//        if(expireTime.isAfter(LocalDateTime.now())){
////          5.1未过期，返回
//            return shop;
//        }
////        5.2过期，重建缓存
////        6.1获取互斥锁
//        String key = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(key);
////        6.2判断是否获取成功
//        if(isLock){
////          6.3成功获取，开启独立线程，将缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {
//                    this.saveShopToRedis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
////                  释放锁
//                    unLock(key);
//                }
//            });
//        }
////        7.返回旧数据
//        return shop;
//    }
/*
    /**
     * 通过设置空值解决缓存穿透问题
     * @param id
     * @return
     */
//    public Shop queryWithPassThrough(Long id){
//        //        1.从redis里查询缓存
//        String s = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(s);
////        2.判断是否存在
//        if(StrUtil.isNotBlank(shopJson)){
////            存在，返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
////        3.判断命中的是否是空值
//        if(shopJson != null){
//            return null;
//        }
////        3.不存在，查询数据库
//        Shop shop = getById(id);
//        if (shop == null){
////            不存在，将空值写入缓存
//            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
////            返回错误信息
//            return null;
//        }
////        5.存在，写入redis
//        stringRedisTemplate.opsForValue().set(s,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
////        6.返回
//        return shop;
//    }

//    private boolean tryLock(String key){
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    private void unLock(String key){
//        stringRedisTemplate.delete(key);
//    }
//
//    /**
//     * 保存逻辑删除的数据
//     * @param id
//     * @param expireSec
//     */
//    public void saveShopToRedis(Long id,Long expireSec) throws InterruptedException {
////        1.查询店铺数据
//        Shop shop = getById(id);
//        Thread.sleep(200);
////        2.封装逻辑过期时间
//        RedisData<Shop> redisData = new RedisData<>();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSec));
////        3.写入redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
//        1.更新数据库
        updateById(shop);
//        2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id) ;
        return Result.ok();
    }
}
