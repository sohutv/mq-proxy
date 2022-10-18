package com.sohu.tv.mq.proxy.test.rocketmq;

import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.test.model.FetchResult;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

/**
 * @author: yongfeigao
 * @date: 2022/7/12 10:09
 */
public class HttpConsumer extends HttpMQ {

    private String requestId;

    protected String uri;

    public HttpConsumer(RestTemplate restTemplate, String group, String topic) {
        super(restTemplate, group, topic);
        uri = "/mq/message?topic=" + topic + "&consumer=" + group + "&requestId={requestId}";
    }

    @Override
    protected long run() {
        URI uri = buildURI();
        MQProxyResponse<FetchResult> response = restTemplate.exchange(uri, HttpMethod.GET, null,
                new ParameterizedTypeReference<MQProxyResponse<FetchResult>>() {
                }).getBody();
        if (response.ok()) {
            FetchResult fetchResult = response.getResult();
            this.requestId = fetchResult.getRequestId();
            return fetchResult.getMsgListSize() + fetchResult.getRetryMsgListSize();
        } else {
            log.warn("consumer:{} response error:{}", group, response.getMessage());
        }
        return 0;
    }

    protected URI buildURI() {
        return restTemplate.getUriTemplateHandler().expand(uri, requestId);
    }

    @Override
    protected void stop() {
        if (requestId == null) {
            return;
        }
        String url = "/mq/ack?topic=" + topic + "&consumer=" + group + "&requestId={requestId}";
        URI uri = restTemplate.getUriTemplateHandler().expand(url, requestId);
        MQProxyResponse<?> response = restTemplate.getForObject(uri, MQProxyResponse.class);
        if (!response.ok()) {
            log.warn("consumer:{} ack error:{}", group, response.getMessage());
        }
    }
}
