package com.example.crocketmqbootc.listener;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * Tag标签 消费者方
 */
@Component
@RocketMQMessageListener(topic = "bootTagTestTopic",
        consumerGroup = "boot-tag-consumer-group",
        selectorType = SelectorType.TAG,// tag过滤模式
        selectorExpression = "tagA || tagB"
//        selectorType = SelectorType.SQL92,// sql92过滤模式
//        selectorExpression = "a in (3,5,7)" // broker.conf中开启enbalePropertyFilter=true
)
public class CTagMsgListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt messageExt) {
        // String keys = messageExt.getKeys();
        System.out.println(new String(messageExt.getBody()));
    }
}
