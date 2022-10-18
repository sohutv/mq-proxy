package com.sohu.tv.mq.proxy.producer.web.param;

import lombok.Data;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

/**
 * 消息参数
 * @author: yongfeigao
 * @date: 2022/6/23 10:19
 */
@Data
public class MessageParam {
    // 生产者
    @NotBlank
    private String producer;

    // 消息体
    @NotBlank
    private String message;

    // 消息keys
    private String keys;

    // 延迟级别
    @Min(1)
    @Max(18)
    private Integer delayLevel;

    // 如果发送失败后，异步重试次数，默认不重试
    @Min(1)
    @Max(10)
    private Integer asyncRetryTimesIfSendFailed;
}
