package com.sohu.tv.mq.proxy.test.rocketmq;

import org.springframework.web.client.RestTemplate;

import java.util.Objects;

/**
 * @author: yongfeigao
 * @date: 2022/7/12 9:35
 */
public class HttpBroadcastingConsumer extends HttpConsumer {

    private String clientId;

    public HttpBroadcastingConsumer(RestTemplate restTemplate, String group, String topic, String clientId) {
        super(restTemplate, group, topic);
        this.clientId = clientId;
        uri = "/mq/message?topic=" + topic + "&consumer=" + group + "&clientId=" + clientId +
                "&requestId={requestId}";
    }

    @Override
    protected String buildRecordKey() {
        return super.buildRecordKey() + ":" + clientId;
    }

    public String getClientId() {
        return clientId;
    }
}
