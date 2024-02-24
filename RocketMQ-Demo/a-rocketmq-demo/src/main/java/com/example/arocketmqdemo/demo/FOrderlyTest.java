package com.example.arocketmqdemo.demo;

import com.example.arocketmqdemo.constant.MqConstant;
import com.example.arocketmqdemo.domain.MsgModel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * 发送顺序消息  1.发送者方需要把消息按照顺序放入同一个队列  2.消费者采用顺序消费的方式 consumer.registerMessageListener(new MessageListenerOrderly() {
 */
public class FOrderlyTest {
    /**
     * 如何保证消息按照顺序消费
     * 1.发送者方需要把消息按照顺序放入同一个队列  2.消费者采用顺序消费的方式(说是单线程模式不太合理,官方说法是顺序消费模式,即一个队列由一个线程去消费;并发消费模式默认20个线程消费4个队列保证不了有序性)
     * (发送顺序消息,或者发送批量消息,都可以让消息放入同一个队列里边)
     */

    private List<MsgModel> msgModels = Arrays.asList(
            new MsgModel("qwer", 1, "下单"),
            new MsgModel("qwer", 1, "短信"),
            new MsgModel("qwer", 1, "物流"),
            new MsgModel("zxcv", 2, "下单"),
            new MsgModel("zxcv", 2, "短信"),
            new MsgModel("zxcv", 2, "物流"),
            new MsgModel("xxxx", 3, "下单"),
            new MsgModel("xxxx", 3, "短信"),
            new MsgModel("xxxx", 3, "物流"),
            new MsgModel("bbbbb", 4, "下单"),
            new MsgModel("bbbbb", 4, "短信"),
            new MsgModel("bbbbb", 4, "物流"),
            new MsgModel("ccc", 5, "下单"),
            new MsgModel("ccc", 5, "短信"),
            new MsgModel("ccc", 5, "物流")
    );

    @Test
    public void orderlyProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("orderly-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        // 发送顺序消息  发送时要确保有序 并且要发到同一个队列下面去
        msgModels.forEach(msgModel -> {
            Message message = new Message("orderlyTopic", msgModel.toString().getBytes());
            try {
                // 发 相同的订单号去相同的队列   msgModel.getOrderSn()传入内部的Object arg
                producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        // 在这里 选择队列
                        int hashCode = arg.toString().hashCode();
                        // 2 % 4 =2
                        // 3 % 4 =3
                        // 4 % 4 =0
                        // 5 % 4 =1
                        // 6 % 4 =2  周期性函数
                        int i = hashCode % mqs.size();
                        /**
                         *  一共4个队列 队列的最大下标3 相同的订单对应的hashcode是相同的 那么计算得到的i的值就相同
                         *  mqs.get(i) 返回的队列就是相同的  做到了相同订单号对应的消息放到来同一个队列中  做到了局部有序即可
                         */
                        return mqs.get(i);
                    }
                }, msgModel.getOrderSn());

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        producer.shutdown();
        System.out.println("发送完成");
    }


    @Test
    public void orderlyConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderly-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("orderlyTopic", "*");
        //consumer.setConsumeThreadMax();默认20个线程 可修改
        // MessageListenerConcurrently 并发模式 多线程的  重试16次
        // MessageListenerOrderly 顺序模式 单线程的(其实是每个队列一个线程)   无限重试Integer.Max_Value
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                System.out.println("线程id:" + Thread.currentThread().getId());
                System.out.println(new String(msgs.get(0).getBody()));
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
        /**
         * 线程id:33
         * 线程id:34
         * 线程id:32
         * MsgModel(orderSn=xxxx, userId=3, desc=下单)
         * 线程id:36
         * MsgModel(orderSn=bbbbb, userId=4, desc=下单)
         * MsgModel(orderSn=zxcv, userId=2, desc=下单)
         * MsgModel(orderSn=qwer, userId=1, desc=下单)
         * 线程id:36
         * 线程id:34
         * 线程id:32
         * 线程id:33
         * MsgModel(orderSn=xxxx, userId=3, desc=短信)
         * MsgModel(orderSn=zxcv, userId=2, desc=短信)
         * MsgModel(orderSn=bbbbb, userId=4, desc=短信)
         * 线程id:33
         * MsgModel(orderSn=qwer, userId=1, desc=短信)
         * 线程id:34
         * MsgModel(orderSn=qwer, userId=1, desc=物流)
         * 线程id:34
         * MsgModel(orderSn=ccc, userId=5, desc=下单)
         * 线程id:36
         * MsgModel(orderSn=xxxx, userId=3, desc=物流)
         * 线程id:32
         * MsgModel(orderSn=zxcv, userId=2, desc=物流)
         * MsgModel(orderSn=bbbbb, userId=4, desc=物流)
         * 线程id:34
         * MsgModel(orderSn=ccc, userId=5, desc=短信)
         * 线程id:34
         * MsgModel(orderSn=ccc, userId=5, desc=物流)
         */
    }


}
