package com.sohu.tv.mq.proxy.consumer.web.param;

import lombok.Data;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import java.util.Set;

/**
 * 消费者配置
 *
 * @author: yongfeigao
 * @date: 2022/6/17 17:55
 */
@Data
public class ConsumerConfigParam {

    @NotBlank
    private String consumer;

    // 每批消息消最大拉取量 最小1，最大128
    @Min(1)
    @Max(128)
    private Integer maxPullSize;

    // 每批消息消费超时毫秒 最小10秒，最大36小时
    @Min(10000)
    @Max(129600000)
    private Long consumeTimeoutInMillis;

    // 消息拉取超时毫秒 最小1秒，最大60秒
    @Min(1000)
    @Max(60000)
    private Long pullTimeoutInMillis;

    // 是否暂停
    @Min(0)
    @Max(1)
    private Integer pause;

    private String clientId;

    // 重置偏移量的时间戳
    @Min(0)
    private Long resetOffsetTimestamp;

    // 是否启用限速
    @Min(0)
    @Max(1)
    private Integer rateLimitEnabled;

    // 限速qps
    @Min(1)
    private Double limitRate;

    // 重试的消息id
    private String retryMsgId;
}
