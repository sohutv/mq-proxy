package com.sohu.tv.mq.proxy.store;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.Pool;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ClusteredRedis
 *
 * @author: yongfeigao
 * @date: 2022/7/22 14:20
 */
public class ClusteredRedis implements IRedis {

    protected JedisCluster jedisCluster;

    public ClusteredRedis() {
    }

    @Override
    public void init(RedisConfiguration redisConfiguration) {
        String[] hostAndPortArray = redisConfiguration.getHost().split(",");
        Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
        for (String hostAndPort : hostAndPortArray) {
            String[] tmpArray = hostAndPort.split(":");
            jedisClusterNode.add(new HostAndPort(tmpArray[0], Integer.parseInt(tmpArray[1])));
        }
        jedisCluster = new JedisCluster(jedisClusterNode, redisConfiguration.getConnectionTimeout(),
                redisConfiguration.getSoTimeout(), 5, redisConfiguration.getPassword(),
                redisConfiguration.getPoolConfig());
    }

    @Override
    public Pool<Jedis> getPool() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    @Override
    public String get(String key) {
        return jedisCluster.get(key);
    }

    @Override
    public String set(String key, String value, SetParams params) {
        return jedisCluster.set(key, value, params);
    }

    @Override
    public Long del(String key) {
        return jedisCluster.del(key);
    }

    @Override
    public Boolean exists(String key) {
        return jedisCluster.exists(key);
    }

    @Override
    public Long incrBy(String key, long increment) {
        return jedisCluster.incrBy(key, increment);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return jedisCluster.hset(key, field, value);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return jedisCluster.hgetAll(key);
    }

    @Override
    public String hget(String key, String field) {
        return jedisCluster.hget(key, field);
    }

    @Override
    public Long hdel(String key, String... field) {
        return jedisCluster.hdel(key, field);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return jedisCluster.hsetnx(key, field, value);
    }

    @Override
    public Long incr(String key) {
        return jedisCluster.incr(key);
    }

    @Override
    public String set(String key, String value) {
        return jedisCluster.set(key, value);
    }
}
