package com.sohu.tv.mq.proxy.store;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.Pool;

import java.util.Map;
import java.util.function.Function;

/**
 * 池化的redis
 *
 * @author: yongfeigao
 * @date: 2022/7/22 14:39
 */
public class PooledRedis implements IRedis {

    private Pool<Jedis> pool;

    public PooledRedis() {
        this.pool = new JedisPool();
    }

    @Override
    public void init(RedisConfiguration redisConfiguration) {
        this.pool = new JedisPool(redisConfiguration.getPoolConfig(),
                redisConfiguration.getHost(),
                redisConfiguration.getPort(),
                redisConfiguration.getConnectionTimeout(),
                redisConfiguration.getSoTimeout(),
                redisConfiguration.getPassword(),
                Protocol.DEFAULT_DATABASE, null);
    }

    @Override
    public Pool<Jedis> getPool() {
        return pool;
    }

    @Override
    public JedisCluster getJedisCluster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String get(String key) {
        return execute(jedis -> jedis.get(key));
    }

    @Override
    public String set(String key, String value, SetParams params) {
        return execute(jedis -> jedis.set(key, value, params));
    }

    @Override
    public Long del(String key) {
        return execute(jedis -> jedis.del(key));
    }

    @Override
    public Boolean exists(String key) {
        return execute(jedis -> jedis.exists(key));
    }

    @Override
    public Long incrBy(String key, long increment) {
        return execute(jedis -> jedis.incrBy(key, increment));
    }

    @Override
    public Long hset(String key, String field, String value) {
        return execute(jedis -> jedis.hset(key, field, value));
    }

    @Override
    public String hget(String key, String field) {
        return execute(jedis -> jedis.hget(key, field));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return execute(jedis -> jedis.hgetAll(key));
    }

    @Override
    public Long hdel(String key, String... field) {
        return execute(jedis -> jedis.hdel(key, field));
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return execute(jedis -> jedis.hsetnx(key, field, value));
    }

    @Override
    public Long incr(String key) {
        return execute(jedis -> jedis.incr(key));
    }

    @Override
    public String set(String key, String value) {
        return execute(jedis -> jedis.set(key, value));
    }

    private <R> R execute(Function<Jedis, R> function) {
        try (Jedis jedis = pool.getResource()) {
            return function.apply(jedis);
        }
    }

    public void setPool(Pool<Jedis> pool) {
        this.pool = pool;
    }
}
