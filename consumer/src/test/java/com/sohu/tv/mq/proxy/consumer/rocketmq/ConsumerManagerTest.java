package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.tv.mq.proxy.consumer.Application;
import com.sohu.tv.mq.proxy.consumer.model.TopicConsumer;
import com.sohu.tv.mq.proxy.consumer.web.param.ConsumerConfigParam;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author: yongfeigao
 * @date: 2022/6/7 11:03
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ConsumerManagerTest {
    String topic = "mqcloud-json-test-topic";
    String consumer = "http-clustering-consumer";

    @Autowired
    private ConsumerManager consumerManager;

    @Test
    public void testRegister() {
        TopicConsumer topicConsumer = new TopicConsumer();
        topicConsumer.setTopic(topic);
        topicConsumer.setConsumer(consumer);
        MQProxyResponse<?> response = consumerManager.register(topicConsumer);
        Assert.assertTrue(!response.ok());
    }

    @Test
    public void testMessageQueueChanged() {
        testRegister();
        consumerManager.messageQueueChanged();
    }
}