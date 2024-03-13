package com.sohu.tv.mq.proxy.consumer.model;

import lombok.Data;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

/**
 * @author: yongfeigao
 * @date: 2022/6/7 11:23
 */
@Data
public class TopicConsumer {
    private String topic;
    private String consumer;
    // 消费方式 0:集群消费,1:广播消费
    private int consumeWay;

    public MessageModel getMessageModel() {
        if (1 == consumeWay) {
            return MessageModel.BROADCASTING;
        }
        return MessageModel.CLUSTERING;
    }
}
