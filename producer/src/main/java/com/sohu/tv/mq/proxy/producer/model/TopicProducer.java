package com.sohu.tv.mq.proxy.producer.model;

import lombok.Data;

/**
 * topic生产者
 *
 * @author: yongfeigao
 * @date: 2022/6/24 15:55
 */
@Data
public class TopicProducer {
    private String topic;
    private String producer;
}
