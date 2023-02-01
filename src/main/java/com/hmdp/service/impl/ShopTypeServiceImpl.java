package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryOrderByAsc() {
        Long size = stringRedisTemplate.opsForList().size(CACHE_SHOP_TYPE_KEY);
        if(size != 0) {
            List<String> range = stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, size);
            ArrayList<ShopType> shopTypes = new ArrayList<>();
            for (String s : range) {
                ShopType shopType = JSONUtil.toBean(s, ShopType.class);
                shopTypes.add(shopType);
            }
            return Result.ok(shopTypes);
        }
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        if(shopTypes == null) {
            return Result.fail("类型不存在");
        }
//        使用stream流将数据转换为JSON字符串
        List<String> shopTypeJSON = shopTypes.stream().sorted(Comparator.comparing(ShopType::getSort))
                .map(JSONUtil::toJsonStr).collect(Collectors.toList());
//        多次访问缓存，性能稍差
//        for (ShopType shopType : shopTypes) {
//            String jsonStr = JSONUtil.toJsonStr(shopType);
//            stringRedisTemplate.opsForList().rightPush(CACHE_SHOP_TYPE_KEY,jsonStr);
//        }
        stringRedisTemplate.opsForList().rightPushAll(CACHE_SHOP_TYPE_KEY,shopTypeJSON);
        stringRedisTemplate.expire(CACHE_SHOP_TYPE_KEY,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(shopTypes);
    }
}
