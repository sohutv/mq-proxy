package com.sohu.tv.mq.proxy.test.rocketmq;

import com.sohu.tv.mq.proxy.producer.web.param.TestParam;
import org.springframework.web.client.RestTemplate;

import java.util.Objects;

/**
 * @author: yongfeigao
 * @date: 2022/7/12 9:35
 */
public class HttpBroadcastingConsumer extends HttpConsumer {

    private String clientId;

    public HttpBroadcastingConsumer(RestTemplate restTemplate, TestParam testParam) {
        super(restTemplate, testParam);
        this.clientId = testParam.getClientId();
        uri += "&clientId=" + clientId;
    }

    @Override
    protected String buildRecordKey() {
        return super.buildRecordKey() + ":" + clientId;
    }

    public String getClientId() {
        return clientId;
    }
}
