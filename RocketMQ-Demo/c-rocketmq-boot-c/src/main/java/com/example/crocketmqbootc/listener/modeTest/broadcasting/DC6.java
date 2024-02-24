package com.example.crocketmqbootc.listener.modeTest.broadcasting;

import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 消息模式 MessageModel   集群模式/广播模式 ; 默认集群模式 常用
 * CLUSTERING[常用] 集群模式下 队列会被消费者分摊, 队列数量>=消费者数量 消息的消费位点 mq服务器会记录处理
 * BROADCASTING 广播模式下 消息会被每一个消费者都处理一次, mq服务器不会记录消费点位,也不会重试
 */
@Component
@RocketMQMessageListener(topic = "modeTopic",
        consumerGroup = "mode-consumer-group-b",
        messageModel = MessageModel.BROADCASTING//广播模式
)
public class DC6 implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        System.out.println("我是mode-consumer-group-b组的第3个消费者:" + message);
    }
}