package com.sohu.tv.mq.proxy.store;

import lombok.Data;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * redis配置
 *
 * @author: yongfeigao
 * @date: 2022/7/22 11:43
 */
@Data
public class RedisConfiguration {
    private GenericObjectPoolConfig<?> poolConfig;
    private String host;
    private int port;
    private int connectionTimeout;
    private int soTimeout;
    private String password;
}
