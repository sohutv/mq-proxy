package com.sohu.tv.mq.proxy.consumer.config;

import com.sohu.tv.mq.proxy.consumer.Application;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.consumer.web.param.ConsumerConfigParam;
import org.apache.rocketmq.client.exception.MQClientException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: yongfeigao
 * @date: 2022/6/24 15:36
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ConsumerConfigManagerTest {

    @Autowired
    private ConsumerManager consumerManager;

    @Autowired
    private ConsumerConfigManager consumerConfigManager;

    @Test
    public void testSetConsumerConfig() throws MQClientException {
        ConsumerConfigParam configParam = new ConsumerConfigParam();
        configParam.setPause(1);
        consumerConfigManager.updateConsumerConfig(configParam);
    }

    @Test
    public void testUpdateRetryMsgId() {
        String consumer = "clustering-mqcloud-http-consumer";
        String msgId = "0A121DC100002A9F00000000214561DD";
        ConsumerProxy consumerProxy = consumerManager.getConsumer(consumer);
        consumerConfigManager.updateRetryMsgId(consumerProxy, msgId);
        List<String> msgIdList = consumerConfigManager.consumeRetryMsgIds(consumer, null, id -> id);
        Assert.assertEquals(msgId, msgIdList.get(0));
    }
}