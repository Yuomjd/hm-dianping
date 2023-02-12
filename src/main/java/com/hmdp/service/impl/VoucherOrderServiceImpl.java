package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {



    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{

        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true){
//                获取队列中的订单信息
                try {
//                    1.获取消息队列中的消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()));
//                    2.判断是否获取成功
                    if(list == null || list.isEmpty()) {
//                    3.如果获取失败，说明没有消息，继续循环
                        continue;
                    }
//                    4.如果获取成功，在数据库中创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
//                    5.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true){
//                获取队列中的订单信息
                try {
//                    1.获取pendingList消息队列中的消息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().
                            read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0")));
//                    2.判断是否获取成功
                    if(list == null || list.isEmpty()) {
//                    3.如果获取失败，说明pendingList中没有消息了，结束循环
                        break;
                    }
//                    4.如果获取成功，在数据库中创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
//                    5.ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("Pending-list里出现异常了");
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }
    /*
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
//                获取队列中的订单信息
                try {
                    VoucherOrder order = orderTasks.take();
                    handleVoucherOrder(order);
                } catch (InterruptedException e) {
                    log.debug("处理订单异常",e);
                }
//                创建订单

            }
        }
    }
     */

    private void handleVoucherOrder(VoucherOrder order) {
        Long userId = order.getUserId();
//        创建锁对象
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
        RLock rLock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = rLock.tryLock();
//        获取锁
        if(!isLock){
//            获取锁失败
            log.error("不允许重复下单");
            return ;
        }
//            获取代理对象
        try {
            proxy.createVoucherOrder(order);
        } finally {
            rLock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckKillVoucher(Long voucherId) {
//        1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        2.判断秒杀是否已经开始
        LocalDateTime beginTime = voucher.getBeginTime();
        if(beginTime.isAfter(LocalDateTime.now())){
            return Result.fail("秒杀尚未开始");
        }
//        3.判断秒杀是否已经结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
//        执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId));
//        判断结果是否为0
        int r = result.intValue();
//        不为0，没有资格
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        返回订单id
        return Result.ok(orderId);
    }


    /* 阻塞队列实现秒杀
    @Override
    public Result seckKillVoucher(Long voucherId) {
//        1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        2.判断秒杀是否已经开始
        LocalDateTime beginTime = voucher.getBeginTime();
        if(beginTime.isAfter(LocalDateTime.now())){
            return Result.fail("秒杀尚未开始");
        }
//        3.判断秒杀是否已经结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
        Long userId = UserHolder.getUser().getId();
//        执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString());
//        判断结果是否为0
        int r = result.intValue();
//        不为0，没有资格
        if(r != 0){
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
//        为0，有资格，将下单信息保存到阻塞队列
//        TODO 保存到阻塞队列
        //        6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
//                  获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
//      向阻塞队列中添加订单
        orderTasks.add(voucherOrder);
//        返回订单id
        return Result.ok(orderId);
    }

     */

    /*
    //旧秒杀
    @Override
    public Result seckKillVoucher(Long voucherId) {
//        1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        2.判断秒杀是否已经开始
        LocalDateTime beginTime = voucher.getBeginTime();
        if(beginTime.isAfter(LocalDateTime.now())){
            return Result.fail("秒杀尚未开始");
        }
//        3.判断秒杀是否已经结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
//        4.判断库存是否为空
        if(voucher.getStock() < 1){
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();
//        创建锁对象
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
        RLock rLock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = rLock.tryLock();
//        获取锁
        if(!isLock){
//            获取锁失败
            return Result.fail("不允许重复下单");
        }
//            获取代理对象
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(userId,voucherId);
        } finally {
            rLock.unlock();
        }
    }

     */

    @NotNull
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //        判断是否已经购买过
            long count = query().eq("user_id", voucherOrder.getUserId()).eq("voucher_id", voucherOrder.getVoucherId()).count();
            if (count > 0) {
                log.error("用户已经购买过了");
                return;
            }
//        5.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
            if (!success) {
                log.error("库存不够了");
                return;
            }
            save(voucherOrder);
        }
}
