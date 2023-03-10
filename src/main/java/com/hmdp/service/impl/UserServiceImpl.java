package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
//        1.校验手机号
        if (!RegexUtils.isPhoneInvalid(phone)) {
            //        2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
//        3.如果符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
//        4.保存验证码到redis //set key value ex 300
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone,code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
//        5.发送验证码
        log.debug("发送短信验证码成功，验证码:{}",code);
//        返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
//        1.校验手机号
        String phone = loginForm.getPhone();
        if (!RegexUtils.isPhoneInvalid(phone)) {
            //        2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
//        2.从redis获取验证码并校验
        String cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if(cacheCode == null || !cacheCode.equals(code)) {
            //        3.不一致，报错
            return Result.fail("验证码错误");
        }
//        4.根据手机号查询用户
        User user = query().eq("phone", phone).one();
//        5.判断用户是否存在
        if(user == null){
//          5.1如果不存在，保存到数据库
            user = createUserWithPhone(phone);
        }
//        6.保存用户到redis
//        6.1生成随机token作为令牌
        String token = UUID.randomUUID().toString(true);
//        6.2将User对象转换为Hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true).setFieldValueEditor((fieldName,fieldValue) -> fieldValue.toString()));
//        6.3存储
        String s = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(s, userMap);
        stringRedisTemplate.expire(s,RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
