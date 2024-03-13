package com.sohu.tv.mq.proxy.producer.web.param;

import lombok.Data;

import org.hibernate.validator.constraints.NotBlank;

/**
 * 消息参数
 * @author: yongfeigao
 * @date: 2022/6/23 10:19
 */
@Data
public class TestParam {
    // topic
    private String topic;
    // 生产者
    @NotBlank
    private String group;

    // clientId
    private String clientId;

    // 运行间隔 ms
    private Integer interval;
}
