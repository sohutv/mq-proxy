package com.sohu.tv.mq.proxy.test.rocketmq;

import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: yongfeigao
 * @date: 2022/7/11 18:09
 */
public class HttpProducer extends HttpMQ {

    private URI uri;

    public static final String MESSAGE = "消息test";

    public static final AtomicLong msgIndex = new AtomicLong();

    public HttpProducer(RestTemplate restTemplate, String group, String topic) {
        super(restTemplate, group, topic);
        uri = restTemplate.getUriTemplateHandler().expand("/mq/produce?producer={producer}", group);
    }

    @Override
    public long run() {
        MultiValueMap<String, String> map = new LinkedMultiValueMap<String, String>();
        String message = MESSAGE + msgIndex.getAndIncrement();
        map.add("message", message);
        HttpEntity<MultiValueMap<String, String>> httpEntity = new HttpEntity<MultiValueMap<String, String>>(map, null);
        while (true) {
            MQProxyResponse<?> response =
                    restTemplate.exchange(uri, HttpMethod.POST, httpEntity, MQProxyResponse.class).getBody();
            if (response.ok()) {
                return 1;
            }
            log.warn("producer:{} retry send message:{}", getGroup(), message);
        }
    }
}
