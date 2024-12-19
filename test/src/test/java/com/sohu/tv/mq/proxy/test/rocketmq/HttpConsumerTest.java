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

    String topic = "mq-http-test-topic";

    String group = "mq-http-cluster-consumer";

    @Test
    public void testConsume() throws InterruptedException {
        HttpConsumer httpConsumer = new HttpConsumer(consumerRestTemplate, group, topic);
        for (int i = 0; i < 100; ++i) {
            long count = httpConsumer.run();
            System.out.println("count:" + count);
            Assert.assertTrue(count >= 0);
            Thread.sleep(1000);
        }
    }

    @Test
    public void testConsumeSupportCookie() throws InterruptedException {
        HttpConsumer httpConsumer = new HttpConsumer(consumerRestTemplate, group, topic, true);
        for (int i = 0; i < 100; ++i) {
            long count = httpConsumer.run();
            System.out.println("count:" + count);
            Assert.assertTrue(count >= 0);
            Thread.sleep(1000);
        }
    }
}