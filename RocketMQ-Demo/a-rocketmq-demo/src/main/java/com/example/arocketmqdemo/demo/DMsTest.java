package com.example.arocketmqdemo.demo;

import com.example.arocketmqdemo.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import java.util.Date;
import java.util.List;

/**
 * 延时消息   常用
 */
public class DMsTest {

    @Test
    public void msProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ms-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("orderMsTopic", "订单号，座位号".getBytes());
        // 给消息设置一个延迟时间
        // messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        message.setDelayTimeLevel(3);
        producer.send(message);
        System.out.println("发送时间" + new Date());
        producer.shutdown();
    }

    /**
     *  第一次加载存在延迟
     * 发送时间Fri Apr 21 16:19:54 CST 2023
     * 收到消息了Fri Apr 21 16:20:20 CST 2023
     * --------------
     * 再次发送消息时则正常
     * 发送时间Tue Feb 20 22:25:25 CST 2024
     * 收到消息了Tue Feb 20 22:25:35 CST 2024
     *
     * @throws Exception
     */
    @Test
    public void msConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ms-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅主题 不设置过滤规则
        consumer.subscribe("orderMsTopic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("收到消息了" + new Date());
                System.out.println(new String(msgs.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }


}


