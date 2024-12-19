package com.sohu.tv.mq.proxy.consumer.model;

import lombok.Data;

import java.util.List;

/**
 * 消费者队列偏移量选择结果
 *
 * @author: yongfeigao
 * @date: 20224/09/18 15:50
 */
@Data
public class ConsumerQueueOffsetResult {
    // 选中的消费者队列偏移量
    private ConsumerQueueOffset chosenConsumerQueueOffset;
    // 消费者队列偏移量列表
    private List<ConsumerQueueOffset> consumerQueueOffsets;
}
