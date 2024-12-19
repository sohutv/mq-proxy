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

    private boolean supportCookie;

    private String requestId;

    protected String uri;

    public HttpConsumer(RestTemplate restTemplate, String group, String topic) {
        this(restTemplate, group, topic, false);
    }

    public HttpConsumer(RestTemplate restTemplate, String group, String topic, boolean supportCookie) {
        super(restTemplate, group, topic);
        this.supportCookie = supportCookie;
        if (supportCookie) {
            uri = "/mq/message?topic=" + topic + "&consumer=" + group;
        } else {
            uri = "/mq/message?topic=" + topic + "&consumer=" + group + "&requestId={requestId}";
        }
    }

    @Override
    protected long run() {
        MQProxyResponse<FetchResult> response = null;
        if (supportCookie) {
            response = restTemplate.exchange(uri, HttpMethod.GET, null,
                    new ParameterizedTypeReference<MQProxyResponse<FetchResult>>() {
                    }).getBody();
        } else {
            URI uri = buildURI();
            response = restTemplate.exchange(uri, HttpMethod.GET, null,
                    new ParameterizedTypeReference<MQProxyResponse<FetchResult>>() {
                    }).getBody();
        }
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
        String url = null;
        if (supportCookie) {
            url = "/mq/ack?topic=" + topic + "&consumer=" + group;
        } else {
            url = "/mq/ack?topic=" + topic + "&consumer=" + group + "&requestId={requestId}";
        }
        URI uri = restTemplate.getUriTemplateHandler().expand(url, requestId);
        MQProxyResponse<?> response = restTemplate.getForObject(uri, MQProxyResponse.class);
        if (!response.ok()) {
            log.warn("consumer:{} ack error:{}", group, response.getMessage());
        }
    }

    public boolean isSupportCookie() {
        return supportCookie;
    }
}
