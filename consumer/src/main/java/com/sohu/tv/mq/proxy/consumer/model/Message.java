package com.sohu.tv.mq.proxy.consumer.model;

import lombok.Data;

/**
 * 消息
 *
 * @author: yongfeigao
 * @date: 2022/6/10 10:26
 */
@Data
public class Message {
    // 消息id
    private String messageId;
    // 消息
    private String message;
    // 消息存储时间
    private long timestamp;
}
