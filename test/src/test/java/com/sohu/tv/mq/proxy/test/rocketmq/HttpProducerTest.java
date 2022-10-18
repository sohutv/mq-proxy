package com.sohu.tv.mq.proxy.test.rocketmq;

import com.sohu.tv.mq.proxy.test.Application;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

/**
 * @author: yongfeigao
 * @date: 2022/7/12 15:14
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class HttpProducerTest {

    @Autowired
    private RestTemplate producerRestTemplate;

    @Test
    public void testProducer() {
        String group = "mqcloud-http-test-topic-producer";
        String topic = "mqcloud-http-test-topic";
        HttpProducer httpProducer = new HttpProducer(producerRestTemplate, group, topic);
        long count = httpProducer.run();
        Assert.assertEquals(1L, count);
    }
}