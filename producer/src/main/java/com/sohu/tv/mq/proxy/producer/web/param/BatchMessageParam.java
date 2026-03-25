package com.sohu.tv.mq.proxy.producer.web.param;

import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * 批量消息参数
 *
 * @author: yongfeigao
 * @date: 2025/2/20
 */
@Data
public class BatchMessageParam {
    // 生产者
    @NotBlank
    private String producer;

    // 消息体
    @NotBlank
    private String messages;
}
