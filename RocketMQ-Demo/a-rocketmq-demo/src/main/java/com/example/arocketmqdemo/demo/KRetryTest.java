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

import java.util.Date;
import java.util.List;
import java.util.UUID;


/**
 *  消息重试  死信消息解决方案
 */
public class KRetryTest {

    /**
     * 当消费重试到达阈值以后，消息不会被投递给消费者了，而是进入了死信队列
     * 当一条消息初次消费失败，RocketMQ会自动进行消息重试，达到最大重试次数后，若消费依然失败，则表明消费者在正常情况下无法正确地消费该消息。
     * 此时，该消息不会立刻被丢弃，而是将其发送到该消费者对应的特殊队列中，这类消息称为死信消息（Dead-Letter Message），存储死信消息的特殊队列称为死信队列（Dead-Letter Queue），
     * 死信队列是死信Topic下分区数唯一的单独队列。如果产生了死信消息，那对应的ConsumerGroup的死信Topic名称为%DLQ%ConsumerGroupName，死信队列的消息将不会再被消费。
     * 可以利用RocketMQ Admin工具或者RocketMQ Dashboard上查询到对应死信消息的信息。我们也可以去监听死信队列，然后进行自己的业务上的逻辑
     */

    /**
     * 重试的时间间隔 默认从第3级别开始
     * 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * 默认重试16次
     * 1.能否自定义重试次数
     * 2.如果重试了16次(并发模式) 顺序模式下(int最大值次)都是失败的?  是一个死信消息 则会放在一个死信主题中去  这个主题中只有一个队列
     * 主题的名称 %DLQ%+消费组名称: 即%DLQ%retry-consumer-group
     * 3.当消息处理失败的时候 该如何正确的处理?
     * 4.第一次消费算重试么?  不算  从第二次开始
     */
    @Test
    public void retryProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("retry-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        // 生产者发送消息 重试次数
        // 同步消息重试次数设置
        producer.setRetryTimesWhenSendFailed(2);
        // 异步消息重试次数设置
        producer.setRetryTimesWhenSendAsyncFailed(2);

        String key = UUID.randomUUID().toString();
        System.out.println(key);
        Message message = new Message("retryTopic", null, key, "消息测试".getBytes());
        producer.send(message);
        System.out.println("发送成功");
        producer.shutdown();
    }

    /**
     * 重试测试
     * @throws Exception
     */
    @Test
    public void retryConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("retryTopic", "*");
        // 设定重试次数
        consumer.setMaxReconsumeTimes(2);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt messageExt = msgs.get(0);
                System.out.println(new Date());
                // 打印重试次数
                System.out.println("重试次数:" + messageExt.getReconsumeTimes());
                System.out.println("消息内容:" + new String(messageExt.getBody()));
                // 业务报错了 返回null 返回RECONSUME_LATER  都会重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        consumer.start();
        System.in.read();
        /**
         * Thu Feb 22 16:54:39 CST 2024
         * 重试次数:0
         * 消息内容:消息测试
         * Thu Feb 22 16:54:49 CST 2024
         * 重试次数:1
         * 消息内容:消息测试
         * Thu Feb 22 16:55:19 CST 2024
         * 重试次数:2
         * 消息内容:消息测试
         */
    }

    // 实际业务中重试的次数一般设置为 5次


    /**
     * 第一种解决方案 直接监听死信主题的消息,记录下来 通知人工接入处理
     * @throws Exception
     */
    @Test
    public void retryDeadConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-dead-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        // 订阅主题 该主题为死信主题
        consumer.subscribe("%DLQ%retry-consumer-group", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt messageExt = msgs.get(0);
                System.out.println(new Date());
                System.out.println(new String(messageExt.getBody()));
                System.out.println("记录到特别的位置 邮件 文件 mysql 通知人工处理");
                // 业务报错了 返回null 返回 RECONSUME_LATER 都会重试
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }


    /**
     * 第二种方案 用法比较多
     * 思考:第一种方案 每个消费者组都要创建一个对应的死信监听方法  如果有几十个组的话 也太繁琐了
     * 所以,我们在消息监听的流程控制中  就直接可以进行重试次数的判断  如果达到设定的阈值  可以直接人工处理,然后消息签收了,不让其做无畏的重试了
     * @throws Exception
     */
    @Test
    public void retryConsumer2() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("retry-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("retryTopic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt messageExt = msgs.get(0);
                System.out.println(new Date());
                try {
                    // 业务处理
                    handleDb();
                } catch (Exception e) {
                    // 业务报错了 进行重试
                    // 获取重试次数
                    int reconsumeTimes = messageExt.getReconsumeTimes();
                    // 重试3次 因为发送时设置来重试次数为2 第一次消费不算重试次数  消费一次 次数+1  0(第一次) 1 2
                    if (reconsumeTimes >= 2) {
                        // 不要重试了
                        System.out.println("记录到特别的位置 文件 mysql 通知人工处理");
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    // 没有达到阈值,则重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                // 业务没有报错,并且执行完毕,签收消息
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
        /**
         * Thu Feb 22 17:17:13 CST 2024
         * Thu Feb 22 17:17:23 CST 2024
         * Thu Feb 22 17:17:53 CST 2024
         * 记录到特别的位置 文件 mysql 通知人工处理
         */
    }

    /**
     * 业务处理方法
     */
    private void handleDb() {
        int i = 10 / 0;
    }

}
