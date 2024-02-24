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

import java.sql.*;
import java.util.List;
import java.util.UUID;

/**
 * 重复消费问题解决方案
 */
public class JRepeatTest {
    /**
     * mq重复消费问题
     * 引起原因:
     * 1.重复投递
     * 2.消费者方扩容时引起重复消费(增加来消费者,队列进行了Rebalance重平衡分配,1一个消费者4个队列[0,1,2,3] 2个消费者时各管理2个队列)
     * (取出消息消费跟消费成功后位点向后移动是两个动作,如果在消费过程中,突然增加了一个消费者,
     * 此时A消息正在消费,位点还没有移动之前,增加的消费者分到了该消息所在队列,发现位点指针指向A消息,则消费)
     *
     *
     * 如下场景:生产者发送订单消息  消费方的业务逻辑是对应减少对应商品的库存操作  如果重复消费则商品的库存数据是不正确的
     * 解决思路,在消费端进行去重处理:
     * 通过redis或者mysql记录消息唯一ID(被消费了的消息需要记录一下,因为这两个容器都有持久化机制,千万不能存内存,分布式项目,多台机器内存不共享)
     * Mysql的新增操作是原子的  如果存在唯一性约束,即使并发插入 最终也只能插入一条成功,后续的都失败
     * 1.新建一个去重表(官方叫法),设置业务字段唯一性约束(比如订单号)
     * 2.每次对消息进行消费的时候,首先取出唯一性业务字段,(执行业务字段订单号的插入操作),如果插入成功,则执行后续的业务代码(减库存),
     * 如果插入失败(违反唯一性约束),说明之前这个消息已经来过了,则不做任何业务操作(消息签收掉即可)
     *
     * 	 * 我们设计一个去重表 对消息的唯一key添加唯一索引
     * 	 * 每次消费消息的时候 先插入数据库 如果成功则执行业务逻辑 [如果业务逻辑执行报错 则删除这个去重表记录,说明消息消费失败了,检查业务逻辑,不要影响后续的插入]
     * 	 * 如果插入失败 则说明消息来过了,直接签收了
     *
     *
     * 下面是官网的原话:
     * 消费过程幂等
     * RocketMQ无法避免消息重复（Exactly-Once），所以如果业务对消费重复非常敏感，务必要在业务层面进行去重处理。可以借助关系数据库进行去重。
     * 首先需要确定消息的唯一键，可以是msgId，也可以是消息内容中的唯一标识字段，例如订单Id等。在消费之前判断唯一键是否在关系数据库中存在。
     * 如果不存在则插入，并消费，否则跳过。（实际过程要考虑原子性问题，判断是否存在可以尝试插入，如果报主键冲突，则插入失败，直接跳过）
     * msgId一定是全局唯一标识符，但是实际使用中，可能会存在相同的消息有两个不同msgId的情况（消费者主动重发、因客户端重投机制导致的重复等），这种情况就需要使业务字段进行重复消费
     */

    /**
     * 消息生产者  上述说的重复消息 不容易模拟出来  这里我们重复发送两条一模一样的消息 模拟重复投递
     * @throws Exception
     */
    @Test
    public void repeatProducer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("repeat-producer-group");
        producer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        producer.start();
        // 设置业务字段订单key
        String key = UUID.randomUUID().toString();
        System.out.println(key);
        // 主题 tag key 消息体
        Message m1 = new Message("repeatTopic", null, key, "扣减库存-1".getBytes());
        Message m2 = new Message("repeatTopic", null, key, "扣减库存-1".getBytes());
        producer.send(m1);
        producer.send(m2);
        System.out.println("发送成功");
        producer.shutdown();
    }

    /**
     * 消费者 通过mysql 唯一性约束  来控制消息重复消费问题
     * @throws Exception
     */
    @Test
    public void repeatConsumer() throws Exception {
        /**
         *      * 幂等性(mysql的唯一索引, redis(setnx) )
         *      * 多次操作产生的影响均和第一次操作产生的影响相同
         *      * 新增:普通的新增操作  是非幂等的,唯一索引的新增,是幂等的
         *      * 修改:看情况
         *      * 查询: 是幂等操作
         *      * 删除:是幂等操作
         *      * ---------------------
         *      * 我们设计一个去重表 对消息的唯一key添加唯一索引
         *      * 每次消费消息的时候 先插入数据库 如果成功则执行业务逻辑 [如果业务逻辑执行报错 则删除这个去重表记录]
         *      * 如果插入失败 则说明消息来过了,直接签收了
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("repeat-consumer-group");
        consumer.setNamesrvAddr(MqConstant.NAME_SRV_ADDR);
        consumer.subscribe("repeatTopic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 先拿key
                MessageExt messageExt = list.get(0);
                String keys = messageExt.getKeys();
                /**
                 * 这里采用原生的jdbc进行数据库操作数据库
                 */
                Connection connection = null;

                try {
                    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?serverTimeZone=GMT%2B8","root","root");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

                PreparedStatement statement = null;
                try {
                    // 插入数据库 因为我们 key做了唯一索引
                    statement = connection.prepareStatement("insert into mq_repeat_consume_test(`order_id`, `desc`) values ('" + keys + "','库存-1')");
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    // 新增 要么成功 要么报错   修改 要么成功,要么返回0 要么报错
                    statement.executeUpdate();
                } catch (SQLIntegrityConstraintViolationException e) {
                    // 唯一索引冲突异常
                    // 说明消息来过了
                    System.out.println("executeUpdate,执行报错SQLIntegrityConstraintViolationException,违反唯一性约束,该消息来过了,直接签收,不进行业务逻辑处理");
                    // 直接签收,不进行业务处理
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }catch (SQLException e){
                    // 其他异常情况
                    e.printStackTrace();
                }

                // 处理业务逻辑
                try{
                    // 如果业务报错 则删除掉这个去重表记录 delete order_oper_log where order_sn = keys;
                    System.out.println("订单ID:"+keys+",消息内容:"+new String(messageExt.getBody()));
                    // 签收
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }catch (Exception e){
                    // 如果业务报错 则删除掉这个去重表记录 delete order_oper_log where order_sn = keys;
                    // 稍后重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });
        consumer.start();
        System.in.read();
        /**
         * 订单ID:78dcba57-f4de-497a-9d35-0d551b37198c,消息内容:扣减库存-1
         * executeUpdate,执行报错SQLIntegrityConstraintViolationException,违反唯一性约束,该消息来过了,直接签收,不进行业务逻辑处理
         */
    }

}
