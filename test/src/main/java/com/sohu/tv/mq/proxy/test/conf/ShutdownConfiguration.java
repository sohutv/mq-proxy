package com.sohu.tv.mq.proxy.test.conf;

import com.sohu.tv.mq.proxy.test.rocketmq.HttpMQManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;

/**
 * @author: yongfeigao
 * @date: 2022/7/12 16:16
 */
@Configuration
public class ShutdownConfiguration {

    @Autowired
    private HttpMQManager httpMQManager;

    @PreDestroy
    public void destroy(){
        httpMQManager.shutdown(true);
    }
}
