package com.sohu.tv.mq.proxy.consumer.offset;

import com.sohu.tv.mq.proxy.consumer.Application;
import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffsetResult;
import com.sohu.tv.mq.proxy.consumer.model.TopicConsumer;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager;
import com.sohu.tv.mq.proxy.store.IRedis;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.SCHEDULE_UPDATE_NEWER_MAX_OFFSET_TOPIC;

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
    private IRedis redis;

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
        ConsumerQueueOffsetResult result = consumerManager.getConsumer(consumer).choose(null);
        Assert.assertNotNull(result);
    }

    @Test
    public void testUpdateNewerMaxOffset() {
        consumerManager.updateNewerMaxOffset(topic);
    }

    @Test
    public void addUpdateNewerMaxOffsetTopic() {
        String value = redis.get(SCHEDULE_UPDATE_NEWER_MAX_OFFSET_TOPIC);
        System.out.println("before:" + value);
        redis.set(SCHEDULE_UPDATE_NEWER_MAX_OFFSET_TOPIC, topic);
        value = redis.get(SCHEDULE_UPDATE_NEWER_MAX_OFFSET_TOPIC);
        System.out.println("after:" + value);
    }
}