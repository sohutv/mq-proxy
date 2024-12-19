package com.sohu.tv.mq.proxy.store;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.Pool;

import java.util.Map;

/**
 * redis统一接口，屏蔽底层实现，所有方法都来自于jedis
 *
 * @author: yongfeigao
 * @date: 2022/7/22 14:08
 */
public interface IRedis {
    /**
     * 初始化
     *
     * @param redisConfiguration
     */
    void init(RedisConfiguration redisConfiguration);

    Pool<Jedis> getPool();

    JedisCluster getJedisCluster();

    String get(String key);

    String set(String key, String value, SetParams params);

    Long del(String key);

    Boolean exists(String key);

    Long incrBy(String key, long increment);

    Long hset(String key, String field, String value);

    Map<String, String> hgetAll(String key);

    String hget(String key, String field);

    Long hdel(String key, String... field);

    Long hsetnx(String key, String field, String value);

    Long incr(String key);

    String set(String key, String value);
}
