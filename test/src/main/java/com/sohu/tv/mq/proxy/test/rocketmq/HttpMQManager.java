package com.sohu.tv.mq.proxy.test.rocketmq;

import com.sohu.tv.mq.proxy.producer.web.param.TestParam;
import com.sohu.tv.mq.proxy.store.IRedis;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author: yongfeigao
 * @date: 2022/7/12 9:29
 */
@Component
public class HttpMQManager {
    @Autowired
    private RestTemplate producerRestTemplate;

    @Autowired
    private RestTemplate consumerRestTemplate;

    @Autowired
    private IRedis redis;

    private List<HttpMQ> instances = new LinkedList<>();

    public void runProducer(TestParam testParam) {
        HttpMQ httpMQ = new HttpProducer(producerRestTemplate, testParam.getGroup(), testParam.getTopic());
        if (testParam.getInterval() != null) {
            httpMQ.setIntervalInMillis(testParam.getInterval());
        }
        runHttpMQ(httpMQ);
    }

    public void runClusteringConsumer(TestParam testParam) {
        HttpMQ httpMQ = new HttpConsumer(consumerRestTemplate, testParam.getGroup(), testParam.getTopic());
        if (testParam.getInterval() != null) {
            httpMQ.setIntervalInMillis(testParam.getInterval());
        }
        runHttpMQ(httpMQ);
    }

    public void runBroadcastingConsumer(TestParam testParam) {
        HttpMQ httpMQ = new HttpBroadcastingConsumer(consumerRestTemplate, testParam.getGroup(), testParam.getTopic(),
                testParam.getClientId());
        if (testParam.getInterval() != null) {
            httpMQ.setIntervalInMillis(testParam.getInterval());
        }
        runHttpMQ(httpMQ);
    }

    public void runHttpMQ(HttpMQ httpMQ) {
        httpMQ.setRedis(redis);
        httpMQ.start();
        instances.add(httpMQ);
    }

    public void stop(String group, String topic) {
        Iterator<HttpMQ> iterator = instances.iterator();
        while (iterator.hasNext()) {
            HttpMQ httpMQ = iterator.next();
            if (httpMQ.getGroup().equals(group) && httpMQ.getTopic().equals(topic)) {
                httpMQ.setRunning(false);
            }
        }
    }

    public void shutdown(boolean force) {
        Iterator<HttpMQ> iterator = instances.iterator();
        while (iterator.hasNext()) {
            HttpMQ httpMQ = iterator.next();
            if (force || !httpMQ.isRunning()) {
                httpMQ.shutdown();
                iterator.remove();
            }
        }
    }

    public List<HttpMQ> getInstances() {
        return instances;
    }
}
