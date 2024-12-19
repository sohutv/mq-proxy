package com.sohu.tv.mq.proxy.producer.rocketmq;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * 轮训选择器
 *
 * @author: yongfeigao
 * @date: 2022/6/23 10:38
 */
public class RoundRobinMessageQueueSelector implements MessageQueueSelector {

    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int value = (int) (((Long) arg) % mqs.size());
        if (value < 0) {
            value = Math.abs(value);
        }
        return mqs.get(value);
    }
}
