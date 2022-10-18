package com.sohu.tv.mq.proxy.consumer.model;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消费者队列偏移量
 *
 * @author: yongfeigao
 * @date: 2022/5/30 9:50
 */
@Data
public class ConsumerQueueOffset {
    private MessageQueue messageQueue;
    // 队列最大偏移量
    private long maxOffset = -1;
    // 提交的偏移量
    private long committedOffset = -1;
    // 锁定的时间戳
    private long lockTimestamp;
    // 上次消费时间戳
    private long lastConsumeTimestamp;
    // 第一次消费
    private boolean firstConsume;

    public ConsumerQueueOffset(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public boolean valid() {
        return maxOffset != -1;
    }
}
