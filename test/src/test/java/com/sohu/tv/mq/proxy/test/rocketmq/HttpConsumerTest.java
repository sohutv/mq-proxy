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
 * @date: 2022/7/12 15:43
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class HttpConsumerTest {

    @Autowired
    private RestTemplate consumerRestTemplate;

    @Test
    public void testConsume() {
        String group = "clustering-mqcloud-http-consumer";
        String topic = "mqcloud-http-test-topic";
        HttpConsumer httpConsumer = new HttpConsumer(consumerRestTemplate, group, topic);
        long count = httpConsumer.run();
        Assert.assertTrue(count > 0);
    }
}