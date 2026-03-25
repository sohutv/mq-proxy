package com.sohu.tv.mq.proxy.producer.rocketmq;

import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.producer.Application;
import com.sohu.tv.mq.proxy.producer.rocketmq.ProducerManager.ProducerProxy;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ProducerManagerTest {

    public static final String PRODUCER = "mq-http-test-topic-producer";

    @Autowired
    private ProducerManager producerManager;

    @Test
    public void testSendBatchOK() {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 10000; ++i) {
            messages.add("test" + i);
        }
        ProducerProxy producerProxy = producerManager.getProducer(PRODUCER);
        MQProxyResponse response = producerProxy.send(messages, null);
        Assert.assertTrue(response.ok());
    }

    @Test
    public void testLargeMessageSendFailed() {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            messages.add("test" + i);
        }
        ProducerProxy producerProxy = producerManager.getProducer(PRODUCER);
        messages.add(buildLargeMessage(producerProxy.getProducer().getProducer().getMaxMessageSize() + 1));
        MQProxyResponse response = producerProxy.send(messages, null);
        Assert.assertTrue(!response.ok());
    }

    @Test
    public void testOneLargeMessageSendFailed() {
        List<String> messages = new ArrayList<>();
        ProducerProxy producerProxy = producerManager.getProducer(PRODUCER);
        messages.add(buildLargeMessage(producerProxy.getProducer().getProducer().getMaxMessageSize() + 1));
        MQProxyResponse response = producerProxy.send(messages, null);
        Assert.assertTrue(!response.ok());
    }

    @Test
    public void testAsyncSendOK() {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            messages.add("test" + i);
        }
        ProducerProxy producerProxy = producerManager.getProducer(PRODUCER);
        messages.add(buildLargeMessage(producerProxy.getProducer().getProducer().getMaxMessageSize()));
        MQProxyResponse response = producerProxy.send(messages, null);
        Assert.assertTrue(response.ok());
    }

    public String buildLargeMessage(int size) {
        char[] largeMessageArray = new char[size];
        Arrays.fill(largeMessageArray, 'a');
        return new String(largeMessageArray);
    }

}