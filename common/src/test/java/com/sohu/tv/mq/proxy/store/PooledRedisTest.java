package com.sohu.tv.mq.proxy.store;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: yongfeigao
 * @date: 2022/7/22 14:35
 */
public class PooledRedisTest {
    @Test
    public void test() {
        RedisConfiguration redisConfiguration = new RedisConfiguration();
        redisConfiguration.setHost("127.0.0.1");
        redisConfiguration.setPort(6379);
        redisConfiguration.setConnectionTimeout(2000);
        redisConfiguration.setSoTimeout(1000);
        redisConfiguration.setPassword("password");
        redisConfiguration.setPoolConfig(new GenericObjectPoolConfig<>());
        PooledRedis pooledRedis = new PooledRedis();
        pooledRedis.init(redisConfiguration);
        Assert.assertNotNull(pooledRedis.getPool());
    }
}