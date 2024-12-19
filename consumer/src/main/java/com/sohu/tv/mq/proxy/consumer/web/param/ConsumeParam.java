package com.sohu.tv.mq.proxy.consumer.web.param;

import lombok.Data;

import javax.validation.constraints.NotBlank;

/**
 * 消费请求参数
 * @author: yongfeigao
 * @date: 2022/6/9 14:23
 */
@Data
public class ConsumeParam {
    @NotBlank
    private String topic;
    @NotBlank
    private String consumer;
    // 广播消费时客户端唯一id
    private String clientId;
    // 确认上次消费成功的标识
    private String requestId;
    // 是否使用cookie
    private boolean useCookie;
    // 客户端ip: 用于记录当前哪个客户端在消费队列
    private String clientIp;

    public boolean isClientIpBlank() {
        if (clientIp == null) {
            return true;
        }
        clientIp = clientIp.trim();
        return clientIp.length() == 0;
    }
}
