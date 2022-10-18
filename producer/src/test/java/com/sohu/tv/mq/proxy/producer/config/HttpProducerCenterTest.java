package com.sohu.tv.mq.proxy.producer.config;

import com.sohu.tv.mq.proxy.producer.Application;
import com.sohu.tv.mq.proxy.producer.model.TopicProducer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Set;

/**
 * @author: yongfeigao
 * @date: 2022/6/24 17:39
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class HttpProducerCenterTest {

    @Autowired
    HttpProducerConfigCenter httpProducerCenter;

    @Test
    public void testFetch(){
        Set<TopicProducer> set = httpProducerCenter.fetchConfigs();
        Assert.assertNotNull(set);
    }

}