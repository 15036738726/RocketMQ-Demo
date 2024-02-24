package com.example.arocketmqdemo.demo;

import com.example.arocketmqdemo.constant.MqConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;

/**
 * 单向消息
 */
public class COnewayTest {
    @Test
    public void onewayProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("oneway-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("onewayTopic","日志xxx".getBytes());
        producer.sendOneway(message);
        System.out.println("成功");
        producer.shutdown();
    }
}
