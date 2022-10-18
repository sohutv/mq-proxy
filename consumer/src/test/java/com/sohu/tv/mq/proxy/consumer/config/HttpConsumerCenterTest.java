package com.sohu.tv.mq.proxy.consumer.config;

import com.sohu.tv.mq.proxy.consumer.Application;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author: yongfeigao
 * @date: 2022/6/24 15:38
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class HttpConsumerCenterTest {

    @Autowired
    private HttpConsumerConfigCenter httpConsumerCenter;
    @Test
    public void testUpdateConsumer() {
        httpConsumerCenter.updateConfig();
    }
}