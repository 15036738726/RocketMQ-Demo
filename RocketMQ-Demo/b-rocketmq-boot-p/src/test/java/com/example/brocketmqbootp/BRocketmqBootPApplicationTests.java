package com.example.brocketmqbootp;

import com.alibaba.fastjson.JSON;
import com.example.brocketmqbootp.domain.MsgModel;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Arrays;
import java.util.List;

@SpringBootTest
class BRocketmqBootPApplicationTests {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Test
    void simpleMsg() {
        // 同步
        SendResult sendResult = rocketMQTemplate.syncSend("bootSyncTestTopic", "我是boot的一个消息");
        System.out.println("同步-发送成功");

        // 异步
        rocketMQTemplate.asyncSend("bootAsyncTestTopic", "我是boot的一个消息", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步-发送成功");
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("异步-发送失败");
            }
        });
        // 单向
        rocketMQTemplate.sendOneWay("bootOnewayTestTopic", "单向消息");
        System.out.println("单向-发送成功");
        // 延迟  org.springframework.messaging.Message;
        Message<String> message = MessageBuilder.withPayload("延时消息").build();
        rocketMQTemplate.syncSend("bootMsTestTopic", message, 3000, 3);
        System.out.println("延迟-发送成功");


    }

    @Test
    public void orderlyMsg() {
        // 顺序
        // 顺序消息 发送者放 需要将一组消息 都发在同一个队列中去  消费者 需要单线程消费
        List<MsgModel> msgModels = Arrays.asList(
                new MsgModel("qwer", 1, "下单"),
                new MsgModel("qwer", 1, "短信"),
                new MsgModel("qwer", 1, "物流"),
                new MsgModel("zxcv", 2, "下单"),
                new MsgModel("zxcv", 2, "短信"),
                new MsgModel("zxcv", 2, "物流")
        );
        msgModels.forEach(e -> {
            // 发送  一般都是以json的方式进行处理
            rocketMQTemplate.syncSendOrderly("bootOrderlyTestTopic", JSON.toJSONString(e), e.getOrderSn());
        });
        System.out.println("顺序-发送成功");
    }

    // 带tag标签的消息
    @Test
    public void tagMsg() {
        // topic:tag
        rocketMQTemplate.syncSend("bootTagTestTopic:tagA", "我是一个带tag的消息");
    }

    // 带key的消息  String keys = messageExt.getKeys();
    @Test
    public void keyMsg() {
        Message<String> message = MessageBuilder.withPayload("我是一个带Key的消息")
                .setHeader(RocketMQHeaders.KEYS, "dfefefeafwaefawe")
                //.setHeader("KEYS","dfefefeafwaefawe")
                .build();
        rocketMQTemplate.syncSend("bootKeyTestTopic", message);
    }


    /**
     * 测试消息模式  MessageModel 集群模式(默认)  广播模式
     * messageModel = MessageModel.CLUSTERING
     * messageModel = MessageModel.BROADCASTING
     * (一定要和消费模式 并发模式/顺序模式区分开  consumeMode = ConsumeMode.CONCURRENTLY 默认并发模式 以上两个都是在消费者方的监听中配置,与发消息无关)
     *
     * @throws Exception
     */
    @Test
    void modeTest() throws Exception {
        for (int i = 1; i <= 5; i++) {
            rocketMQTemplate.syncSend("modeTopic", "我是第" + i + "个消息");
        }
        System.out.println("发送成功");
    }

    // 积压问题
    /**
     * 如何解决消息堆积问题?
     *
     * 产生原因:
     * 第一种情况.生产太快了,生产消息的速度远远大于处理消息的速度
     * 第二种情况.消费者消费出现问题
     * 第一种情况:
     * 生产方可以做业务限流;
     * 增加消费者数量,但是消费者数量<=队列数量;
     * 动态扩容队列数量,默认4个队列,从而增加消费者数量;
     * 根据消费方处理的任务类型,适当的设置消费者线程数量,默认是20,(IO密集型(2n),CPU密集型(n+1)) IO:磁盘操作较多,大量的sql交互   CUP:计算多  n是当前机器的最大线程数,动态获取Runtime.getRuntime().availableProcessors() 不要写死;
     * 官网原话:
     * 同一个 ConsumerGroup 下，通过增加 Consumer 实例数量来提高并行度（需要注意的是超过订阅队列数的 Consumer 实例无效）。可以通过加机器，或者在已有机器启动多个进程的方式。
     * 提高单个 Consumer 的消费并行线程，通过修改参数 consumeThreadNumber实现。
     *
     * 如果是第二种情况:
     * 排查消费者程序的问题(程序报错,导致消息签收失败,频繁重试消费);
     * 程序自身存在bug,执行时间过长,比如,存在慢sql交互;
     */
    /**
     *     @RocketMQMessageListener(topic = "",
     *             consumerGroup = "",
     *             consumeThreadNumber = 40,
     *             consumeMode = ConsumeMode.CONCURRENTLY)
     */


    /**
     * 如何确保消息不丢失?[面试重要]
     * 产生原因:异步刷盘机制;	一共两种机制,同步刷盘/异步刷盘机制(mq内部持久化机制)
     *
     * 发送同步消息流程:消息由生产者发送到 borker,然后进行同步刷盘成功之后(mq内部的存储机制,采用的是顺序读写,速度也不慢) 然后发送结果给生产者,这种发送一般消息时不会丢失的,除非磁盘坏了
     * 他的思想就是borker接受到消息,并且对消息执行刷盘持久化之后,才返回发送成功给生产者
     * 而发送异步消息,则是把消息先放入缓冲区(内存,然后通知生产者数据发送成功),待一定时机才进行刷盘动作,而且mq模式的刷盘机制就是异步刷盘,一旦宕机,这些内存中缓冲的消息就会有丢失的风险,下次重启就找不到了
     * 解决办法如下几种:
     * 第一种.生产者使用同步发送模式(性能不太好,一般都是用异步)
     * 第二种.将mq的刷盘机制设置为同步刷盘(不推荐)
     *
     * 第三种推荐:那么如果采用异步发送方式该如何处理呢?
     * 思想,自己记录消息(不在依赖于mq的持久化,自己记录)
     * 发送方异步发送,在发送方收到发送成功通知的时候,在异步回调方法内部执行一个数据持久化动作
     * insert 消息业务ID,状态=1(标识未消费),创建时间
     * 消费者方,处理完业务逻辑之后,更新这个消息的状态为0(标识已经消费)
     * update  状态=0,消费时间
     * 然后每间隔一段时间去查询表中的这些记录(通过定时任务),那些消息是发了很久的,但是没有被消费的,那么这些消息是不是就是丢失了?
     * 然后发送者方,通过表中的数据可以进行消息补发即可,消费者方只要保证好幂等性原则(不要重复消费,因为消息可能在mq中排队,还没有来得消费呢),就可以了
     * 如上被补发的消息大概由以下两种原因:
     * 1.P端生产者,异步发送的消息,在缓冲区的消息在刷盘之前丢失
     * 2.C端消费者,没有及时消费的消息(所以C端一定要控制好幂等性操作)
     */


}
