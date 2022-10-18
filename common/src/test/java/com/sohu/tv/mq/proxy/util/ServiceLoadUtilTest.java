package com.sohu.tv.mq.proxy.util;

import com.sohu.tv.mq.proxy.store.IRedis;
import com.sohu.tv.mq.proxy.store.PooledRedis;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: yongfeigao
 * @date: 2022/7/22 14:40
 */
public class ServiceLoadUtilTest {

    @Test
    public void test() {
        IRedis redis = ServiceLoadUtil.loadService(IRedis.class, PooledRedis.class);
        Assert.assertEquals(PooledRedis.class, redis.getClass());
    }
}