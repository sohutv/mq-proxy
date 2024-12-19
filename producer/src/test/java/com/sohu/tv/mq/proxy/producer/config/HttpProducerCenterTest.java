package com.sohu.tv.mq.proxy.producer.config;

import com.sohu.tv.mq.proxy.producer.Application;
import com.sohu.tv.mq.proxy.producer.model.TopicProducer;
import com.sohu.tv.mq.proxy.store.IRedis;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.exceptions.JedisDataException;

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

    @Autowired
    private IRedis redis;

    @Test
    public void testFetch(){
        Set<TopicProducer> set = httpProducerCenter.fetchConfigs();
        Assert.assertNotNull(set);
    }

    @Test(expected = JedisDataException.class)
    public void testIncrOverflow(){
        String key = "test";
        redis.set(key, String.valueOf(Long.MAX_VALUE));
        redis.incr(key);
    }

    @Test
    public void testIncrOverflowRest() {
        String key = "test";
        redis.set(key, String.valueOf(Long.MAX_VALUE));
        try {
            redis.incr(key);
        } catch (JedisDataException e) {
            redis.set(key, "0");
        }
        Assert.assertEquals(Long.valueOf(1), redis.incr(key));
    }
}