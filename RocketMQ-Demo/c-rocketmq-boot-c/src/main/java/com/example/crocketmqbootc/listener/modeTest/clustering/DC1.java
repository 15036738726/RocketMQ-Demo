package com.example.crocketmqbootc.listener.modeTest.clustering;

import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 消息模式 MessageModel   集群模式/广播模式 ; 默认集群模式 常用
 * CLUSTERING[常用] 集群模式下 队列会被消费者分摊,  队列数量>=消费者数量(=时 每个消费者分到1个队列)  消息的消费位点 mq服务器会记录处理
 * BROADCASTING 广播模式下 消息会被每一个消费者都处理一次, mq服务器不会记录消费点位,也不会重试
 */
@Component
@RocketMQMessageListener(topic = "modeTopic",
        consumerGroup = "mode-consumer-group-a",
        messageModel = MessageModel.CLUSTERING//集群模式
)
public class DC1 implements RocketMQListener<String> {
    // DC1 DC2 DC3 都是一个消费者组  采用集群模式
    @Override
    public void onMessage(String message) {
        System.out.println("我是mode-consumer-group-a组的第1个消费者:" + message);
    }
    /**
     * 一共发了5条消息  b组中的3个消费者 每个消费者都消费了5条消息
     * 而a组 三个消费者的消费情况: 第2个消费者:2  第1个消费者:1  第3个消费者:2   即同一个消费组内的消费者分担消费
     * 我是mode-consumer-group-b组的第2个消费者:我是第2个消息
     * 我是mode-consumer-group-b组的第2个消费者:我是第1个消息
     * 我是mode-consumer-group-b组的第1个消费者:我是第2个消息
     * 我是mode-consumer-group-b组的第3个消费者:我是第1个消息
     * 我是mode-consumer-group-b组的第1个消费者:我是第1个消息
     * 我是mode-consumer-group-b组的第3个消费者:我是第2个消息
     * 我是mode-consumer-group-a组的第3个消费者:我是第1个消息
     * 我是mode-consumer-group-a组的第2个消费者:我是第2个消息
     * 我是mode-consumer-group-b组的第3个消费者:我是第3个消息
     * 我是mode-consumer-group-a组的第2个消费者:我是第3个消息
     * 我是mode-consumer-group-b组的第2个消费者:我是第3个消息
     * 我是mode-consumer-group-b组的第1个消费者:我是第3个消息
     * 我是mode-consumer-group-b组的第3个消费者:我是第4个消息
     * 我是mode-consumer-group-b组的第2个消费者:我是第4个消息
     * 我是mode-consumer-group-a组的第1个消费者:我是第4个消息
     * 我是mode-consumer-group-b组的第1个消费者:我是第4个消息
     * 我是mode-consumer-group-b组的第3个消费者:我是第5个消息
     * 我是mode-consumer-group-b组的第2个消费者:我是第5个消息
     * 我是mode-consumer-group-a组的第3个消费者:我是第5个消息
     * 我是mode-consumer-group-b组的第1个消费者:我是第5个消息
     */
}