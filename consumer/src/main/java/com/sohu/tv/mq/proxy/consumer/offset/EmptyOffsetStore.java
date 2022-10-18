package com.sohu.tv.mq.proxy.consumer.offset;

import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;
import java.util.Set;

/**
 * 空实现
 * @author: yongfeigao
 * @date: 2022/5/31 15:08
 */
public class EmptyOffsetStore implements OffsetStore {
    @Override
    public void load() throws MQClientException {
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
    }

    @Override
    public long readOffset(MessageQueue mq, ReadOffsetType type) {
        return 0;
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
    }

    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeOffset(MessageQueue mq) {
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        return null;
    }

    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
    }
}
