package com.sohu.tv.mq.proxy.consumer.config;

import com.sohu.tv.mq.proxy.config.MQConfigCenter;
import com.sohu.tv.mq.proxy.consumer.model.TopicConsumer;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
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
 * 用于从mqcloud获取http消费者
 *
 * @author: yongfeigao
 * @date: 2022/6/24 11:12
 */
@Order(1)
@Component
public class HttpConsumerConfigCenter extends MQConfigCenter<TopicConsumer> implements ApplicationRunner {

    @Autowired
    private RestTemplate mqCloudRestTemplate;

    @Autowired
    private ConsumerManager consumerManager;

    @Override
    public void deleteConfig(TopicConsumer config) {
        consumerManager.unregister(config.getConsumer());
    }

    @Override
    public void addConfig(TopicConsumer config) {
        // 防止之前已经注册过
        consumerManager.unregister(config.getConsumer());
        consumerManager.register(config);
    }

    @Override
    protected Set<TopicConsumer> fetchConfigs() {
        MQProxyResponse<Set<TopicConsumer>> response = mqCloudRestTemplate.exchange("/topic/httpConsumer",
                HttpMethod.GET, null,
                new ParameterizedTypeReference<MQProxyResponse<Set<TopicConsumer>>>() {
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
