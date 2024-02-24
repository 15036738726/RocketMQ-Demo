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

/**
 * 发送带标签的消息，消息过滤
 */
public class GTagTest {

    /**
     * 举个例子  普通快递订单消息   生鲜快递订单消息   他们都属于订单消息  可以使用同一个topic主题
     * tagOrderTopic
     * 然后设置两个消费者组 都订阅tagOrderTopic  一个消费者组消费 普通快递订单消息 ;一个消费者组消费 生鲜快递订单消息
     *
     * 普通快递消费者组 putong-order-group
     * 生鲜快递消费者组 shengxian-order-group
     *
     * 他们都订阅tagOrderTopic   consumer.subscribe("tagOrderTopic ")
     *
     * 因为是两个组 所以发送的订单消息会每个消费组给一份
     * 所以组内可以通过tag继续进行区分
     *
     * 最终通过控制台  可以看到 每个组内一共收到两条消息  而且都消费了
     * tagOrderTopic订阅组
     *              putong-order-group
     *              Broker	    队列	消费者终端	                    代理者位点	        消费者位点	差值	上次时间
     *              localhost	2	192.168.31.222@12900#61358009511800	1	                 1	        0	2024-02-21 15:40:26
     *              localhost	3	192.168.31.222@12900#61358009511800	1	                 1	        0	2024-02-21 15:40:26
     *
     *              shengxian-order-group
     *              Broker	    队列	消费者终端	                    代理者位点	        消费者位点	差值	上次时间
     *              localhost	2	192.168.31.222@7836#61363084483200	1	                1	        0	2024-02-21 15:40:26
     *              localhost	3	192.168.31.222@7836#61363084483200	1	                1	        0	2024-02-21 15:40:26
     * 而且都消费了,但是通过tar标识  每个消费者只处理对应自己的消息 各自跳过不属于自己的消息
     *
     * @throws Exception
     */

    // 下面写个demo测试模拟一下如上场景

    /**
     * 订单生产者  生成普通订单(服装类快递,时间要求不高,3天内到达)  以及 生鲜订单(发货快,时间要求很高,半小时到达)
     * @throws Exception
     */
    @Test
    public void tagOrderProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tag-order-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("tagOrderTopic", "slowKuaiDi", "普通快递-衣服".getBytes());
        Message message2 = new Message("tagOrderTopic", "fastKuaiDi", "盒马生鲜-红烧鱼".getBytes());
        producer.send(message);
        producer.send(message2);
        System.out.println("发送成功");
        producer.shutdown();
    }

    /**
     * 我是[普通]快递消费者，我正在消费消息普通快递-衣服
     * @throws Exception
     */
    @Test
    public void tagPuTongOrderConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("putong-order-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("tagOrderTopic","slowKuaiDi");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("我是[普通]快递消费者，我正在消费消息" + new String(list.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    /**
     * 我是[生鲜]快递消费者，我正在消费消息盒马生鲜-红烧鱼
     * @throws Exception
     */
    @Test
    public void tagShenXianOrderConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("shengxian-order-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("tagOrderTopic","fastKuaiDi");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("我是[生鲜]快递消费者，我正在消费消息" + new String(list.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }



    //////////////////////////////////////////下面是老师写的demo////////////////////////////////////////////////////

    @Test
    public void tagProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("tag-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        Message message = new Message("tagTopic", "vip1", "我是vip1的文章".getBytes());
        Message message2 = new Message("tagTopic", "vip2", "我是vip2的文章".getBytes());
        producer.send(message);
        producer.send(message2);
        System.out.println("发送成功");
        producer.shutdown();
    }

    /**
     * 我是vip1的消费者，我正在消费消息我是vip1的文章
     * @throws Exception
     */
    @Test
    public void tagConsumer1() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-a");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("tagTopic","vip1");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("我是vip1的消费者，我正在消费消息" + new String(list.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }

    /**
     * 我是vip2的消费者，我正在消费消息我是vip2的文章
     * 我是vip2的消费者，我正在消费消息我是vip1的文章
     * @throws Exception
     */
    @Test
    public void tagConsumer2() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("tag-consumer-group-b");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("tagTopic","vip1 || vip2");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("我是vip2的消费者，我正在消费消息" + new String(list.get(0).getBody()));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.in.read();
    }
}
