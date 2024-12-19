package com.sohu.tv.mq.proxy.producer.conf;

import com.sohu.tv.mq.proxy.store.IRedis;
import com.sohu.tv.mq.proxy.store.PooledRedis;
import com.sohu.tv.mq.proxy.store.RedisConfiguration;
import com.sohu.tv.mq.proxy.util.ServiceLoadUtil;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.OkHttp3ClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.TimeUnit;

/**
 * 通用配置
 *
 * @author: yongfeigao
 * @date: 2022/6/1 10:17
 */
@Configuration
public class CommonConfiguration {

    @Value("${mqcloud.domain}")
    private String mqcloudDomain;

    @Bean
    @ConfigurationProperties("redis")
    public RedisConfiguration redisConfiguration() {
        return new RedisConfiguration();
    }

    @Bean
    public IRedis redis(RedisConfiguration redisConfiguration) {
        IRedis redis = ServiceLoadUtil.loadService(IRedis.class, PooledRedis.class);
        redis.init(redisConfiguration);
        return redis;
    }

    @Bean
    public RestTemplate mqCloudRestTemplate(RestTemplateBuilder restTemplateBuilder) {
        RestTemplate restTemplate =
                restTemplateBuilder.requestFactory(() -> new OkHttp3ClientHttpRequestFactory(new OkHttpClient.Builder()
                        .connectionPool(new ConnectionPool())
                        .connectTimeout(2000, TimeUnit.MILLISECONDS)
                        .readTimeout(1000, TimeUnit.MILLISECONDS)
                        .writeTimeout(1000, TimeUnit.MILLISECONDS).build()))
                        .rootUri("http://" + mqcloudDomain).build();
        return restTemplate;
    }
}
