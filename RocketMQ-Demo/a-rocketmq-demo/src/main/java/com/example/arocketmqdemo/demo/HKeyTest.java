package com.example.arocketmqdemo.demo;

import com.example.arocketmqdemo.constant.MqConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

/**
 * 在rocketmq中的消息，默认会有一个messageId当做消息的唯一标识，
 * 我们也可以给消息携带一个key，用作唯一标识或者业务标识，包括在控制面板查询的时候也可以使用messageId或者key来进行查询
 * 未指定的情况下  key默认是""
 */
public class HKeyTest {


    /**
     * mq自己默认的消息MsgID是没有任何业务意义的,它只保证了唯一性
     * key 可作为业务参数 我们自身要确保唯一,无论是否唯一,mq都能发送成功,对于重要的消息(比如订单)
     * 为了查阅和去重  通常自己指定
     *
     * @throws Exception
     */

    @Test
    public void keyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("key-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        String key = UUID.randomUUID().toString();
        System.out.println(key);
        Message message = new Message("keyTopic","a",key,"自定义key的消息".getBytes());
        producer.send(message);
        System.out.println("发送成功");
        producer.shutdown();
    }
    @Test
    public void keyConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("key-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("keyTopic","*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                MessageExt messageExt = list.get(0);
                System.out.println("消费消息:" + new String(messageExt.getBody()));
                String keys = messageExt.getKeys();
                System.out.println(keys);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }
}
