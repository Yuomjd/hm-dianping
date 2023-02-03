package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@Component
public class RedisIdWorker {

    private static final long BEGIN_TIMESTAMP = 1672531200L;

    private static final int COUNT_BITS = 32;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    public long nextId(String keyPreFix) {
//        1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long second = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = second - BEGIN_TIMESTAMP;
//        2.生成序列号
//        获取当前日期
        String date = now.format(DateTimeFormatter.ofPattern("yyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPreFix + ":" + date);
//        3.拼接后返回

        return timestamp << COUNT_BITS | count;
    }
}
