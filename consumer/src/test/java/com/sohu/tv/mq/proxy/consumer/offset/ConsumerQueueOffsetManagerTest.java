package com.sohu.tv.mq.proxy.consumer.offset;

import com.sohu.tv.mq.proxy.consumer.Application;
import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffset;
import com.sohu.tv.mq.proxy.consumer.model.TopicConsumer;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author: yongfeigao
 * @date: 2022/6/13 18:22
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ConsumerQueueOffsetManagerTest {

    String topic = "mqcloud-json-test-topic";
    String consumer = "http-clustering-consumer";

    @Autowired
    private ConsumerManager consumerManager;

    @Before
    public void before() {
        TopicConsumer topicConsumer = new TopicConsumer();
        topicConsumer.setTopic(topic);
        topicConsumer.setConsumer(consumer);
        consumerManager.register(topicConsumer);
    }

    @Test
    public void testChoose() throws MQClientException {
        ConsumerQueueOffset consumerQueueOffset = consumerManager.getConsumer(consumer).choose(null);
        Assert.assertNotNull(consumerQueueOffset);
    }
}