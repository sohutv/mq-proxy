package com.sohu.tv.mq.proxy.test.web.controller;

import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.producer.web.param.TestParam;
import com.sohu.tv.mq.proxy.test.rocketmq.HttpMQManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * MQController
 *
 * @author: yongfeigao
 * @date: 2022/6/6 17:57
 */
@RestController
@RequestMapping("/test")
@CrossOrigin
public class TestController {

    @Autowired
    private HttpMQManager httpMQManager;

    @GetMapping("/produce")
    public MQProxyResponse<?> produce(@Valid TestParam testParam) throws Exception {
        httpMQManager.runProducer(testParam);
        return MQProxyResponse.buildOKResponse();
    }

    @GetMapping("/consume/clustering")
    public MQProxyResponse<?> consumeClustering(@Valid TestParam testParam) throws Exception {
        httpMQManager.runClusteringConsumer(testParam);
        return MQProxyResponse.buildOKResponse();
    }

    @GetMapping("/consume/broadcasting")
    public MQProxyResponse<?> consumeBroadcasting(@Valid TestParam testParam) throws Exception {
        httpMQManager.runBroadcastingConsumer(testParam);
        return MQProxyResponse.buildOKResponse();
    }

    @GetMapping("/stop")
    public MQProxyResponse<?> stop(@Valid TestParam testParam) throws Exception {
        httpMQManager.stop(testParam.getGroup(), testParam.getTopic());
        return MQProxyResponse.buildOKResponse();
    }

    @GetMapping("/remove/stopped")
    public MQProxyResponse<?> removeStopped() throws Exception {
        httpMQManager.shutdown(false);
        return MQProxyResponse.buildOKResponse();
    }

    @GetMapping("/auto")
    public MQProxyResponse<?> produceAuto(TestParam testParam) throws Exception {
        testParam.setTopic("mqcloud-http-test-topic");
        testParam.setGroup("mqcloud-http-test-topic-producer");
        testParam.setInterval(100);
        httpMQManager.runProducer(testParam);

        testParam.setGroup("clustering-mqcloud-http-consumer");
        testParam.setInterval(1000);
        httpMQManager.runClusteringConsumer(testParam);
        httpMQManager.runClusteringConsumer(testParam);

        testParam.setGroup("broadcast-mqcloud-http-consumer");
        testParam.setClientId("127.0.0.1");
        httpMQManager.runBroadcastingConsumer(testParam);

        testParam.setClientId("127.0.0.2");
        httpMQManager.runBroadcastingConsumer(testParam);
        return MQProxyResponse.buildOKResponse();
    }

    @GetMapping("/remove/all")
    public MQProxyResponse<?> removeAll() throws Exception {
        httpMQManager.shutdown(true);
        return MQProxyResponse.buildOKResponse();
    }

    @GetMapping("/stats")
    public MQProxyResponse<?> stats() throws Exception {
        return MQProxyResponse.buildOKResponse(httpMQManager.getInstances());
    }
}
