package com.example.crocketmqbootc.listener;

import com.alibaba.fastjson.JSON;
import com.example.crocketmqbootc.domain.MsgModel;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 顺序消费  模式需要修改为单线程  并且需要设置重试次数  否则Integer.max
 */
@Component
@RocketMQMessageListener(topic = "bootOrderlyTestTopic", consumerGroup = "boot-orderly-consumer-group",
        consumeMode = ConsumeMode.ORDERLY,// 顺序消费模式 设置为单线程
        maxReconsumeTimes = 5)// 消费重试的次数
public class BOrderlyMsgListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt messageExt) {
        MsgModel msgModel = JSON.parseObject(new String(messageExt.getBody()), MsgModel.class);
        System.out.println(msgModel);
        /**
         * 相同的订单是有序的  局部有序
         * MsgModel(orderSn=qwer, userId=1, desc=下单)
         * MsgModel(orderSn=zxcv, userId=2, desc=下单)
         * MsgModel(orderSn=qwer, userId=1, desc=短信)
         * MsgModel(orderSn=qwer, userId=1, desc=物流)
         * MsgModel(orderSn=zxcv, userId=2, desc=短信)
         * MsgModel(orderSn=zxcv, userId=2, desc=物流)
         */
    }
}
