package com.example.arocketmqdemo.demo;

import com.example.arocketmqdemo.constant.MqConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.Test;


/**
 * 发送异步消息   常用
 */
public class BASyncTest {

    @Test
    public void asyncProducer() throws Exception {
        // 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("async-producer-group");
        // 连接namesrv
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 启动
        producer.start();
        // 创建消息
        Message message = new Message("asyncTopic","我是一个异步消息".getBytes());

        // 发送消息  消息发出后 当前代码不阻塞  继续向下执行
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送成功");
            }

            @Override
            public void onException(Throwable e) {
                System.err.println("发送失败:" + e.getMessage());
            }
        });

        System.out.println("主程序执行完毕");
        System.in.read();
    }

}
