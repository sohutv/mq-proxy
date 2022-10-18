package com.sohu.tv.mq.proxy.model;

/**
 * 消费异常
 * @author: yongfeigao
 * @date: 2022/6/13 14:37
 */
public class MQException extends RuntimeException {
    public MQException(String message) {
        super(message);
    }
}
