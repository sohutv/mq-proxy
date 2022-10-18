package com.sohu.tv.mq.proxy.producer.web.controller;

import com.sohu.index.tv.mq.common.Result;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.producer.rocketmq.ProducerManager;
import com.sohu.tv.mq.proxy.producer.rocketmq.ProducerManager.ProducerProxy;
import com.sohu.tv.mq.proxy.producer.web.param.MessageParam;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
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
@RequestMapping("/mq")
@CrossOrigin
public class MQController {

    @Autowired
    private ProducerManager producerManager;

    /**
     * 生产消息
     *
     * @param messageParam
     * @return
     * @throws Exception
     */
    @PostMapping("/produce")
    public MQProxyResponse<?> produce(@Valid MessageParam messageParam) throws Exception {
        ProducerProxy producerProxy = producerManager.getProducer(messageParam.getProducer());
        Result<SendResult> result = producerProxy.send(messageParam);
        if (!result.isSuccess()) {
            if (!result.isRetrying()) {
                return MQProxyResponse.buildErrorResponse(result.getException().toString());
            } else {
                return MQProxyResponse.buildUnknownResponse(result.getException().toString());
            }
        }
        return MQProxyResponse.buildOKResponse(result.getResult());
    }
}
