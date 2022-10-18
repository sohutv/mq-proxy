package com.sohu.tv.mq.proxy.producer.config;

import com.sohu.tv.mq.proxy.config.MQConfigCenter;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.producer.model.TopicProducer;
import com.sohu.tv.mq.proxy.producer.rocketmq.ProducerManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Set;

/**
 * 用于从mqcloud获取http生产者
 *
 * @author: yongfeigao
 * @date: 2022/6/24 11:12
 */
@Order(1)
@Component
public class HttpProducerConfigCenter extends MQConfigCenter<TopicProducer> implements ApplicationRunner {

    @Autowired
    private RestTemplate mqCloudRestTemplate;

    @Autowired
    private ProducerManager producerManager;

    @Override
    public void deleteConfig(TopicProducer config) {
        producerManager.unregister(config.getProducer());
    }

    @Override
    public void addConfig(TopicProducer config) {
        producerManager.register(config);
    }

    @Override
    protected Set<TopicProducer> fetchConfigs() {
        MQProxyResponse<Set<TopicProducer>> response = mqCloudRestTemplate.exchange("/topic/httpProducer",
                HttpMethod.GET, null,
                new ParameterizedTypeReference<MQProxyResponse<Set<TopicProducer>>>() {
                }).getBody();
        if (!response.ok()) {
            throw new MQException(response.toString());
        }
        return response.getResult();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("updateConfig");
        updateConfig();
    }
}
