package com.sohu.tv.mq.proxy.consumer.web.controller;

import com.sohu.tv.mq.proxy.consumer.config.ConsumerConfigManager;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.FetchResult;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.consumer.rocketmq.MessageFetcher;
import com.sohu.tv.mq.proxy.consumer.web.param.ConsumeParam;
import com.sohu.tv.mq.proxy.consumer.web.param.ConsumerConfigParam;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.util.WebUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

/**
 * MQController
 *
 * @author: yongfeigao
 * @date: 2022/6/6 17:57
 */
@Slf4j
@RestController
@RequestMapping("/mq")
@CrossOrigin
public class MQController {

    @Autowired
    private MessageFetcher messageFetcher;

    @Autowired
    private ConsumerManager consumerManager;

    @Autowired
    private ConsumerConfigManager consumerConfigManager;

    /**
     * 拉取消息
     *
     * @throws Exception
     */
    @RequestMapping("/message")
    public MQProxyResponse<?> message(@Valid ConsumeParam param, HttpServletRequest request, HttpServletResponse response) throws Exception {
        getRequestIdFromCookie(param, request);
        MQProxyResponse<?> mqProxyResponse = messageFetcher.fetch(FetchRequest.build(request, param));
        saveRequestIdToCookie(param, mqProxyResponse, response);
        return mqProxyResponse;
    }

    /**
     * 从cookie获取request id
     */
    private void getRequestIdFromCookie(ConsumeParam param, HttpServletRequest request) {
        if (param.isUseCookie() && StringUtils.isEmpty(param.getRequestId())) {
            param.setRequestId(WebUtil.getRequestIdFromCookie(request));
        }
    }

    /**
     * 保存request id到cookie
     */
    private void saveRequestIdToCookie(ConsumeParam param, MQProxyResponse<?> mqProxyResponse, HttpServletResponse response) {
        if (param.isUseCookie() && mqProxyResponse.ok()) {
            FetchResult fetchResult = (FetchResult) mqProxyResponse.getResult();
            WebUtil.setRequestIdToCookie(response, fetchResult.getRequestId());
        }
    }

    /**
     * ack
     *
     * @param param
     * @return
     * @throws Exception
     */
    @RequestMapping("/ack")
    public MQProxyResponse<String> ack(@Valid ConsumeParam param, HttpServletRequest request) throws Exception {
        getRequestIdFromCookie(param, request);
        FetchRequest fetchRequest = FetchRequest.build(param);
        // 获取消费代理
        ConsumerProxy consumer = consumerManager.getConsumer(fetchRequest);
        // offset ack
        return consumer.offsetAck(fetchRequest);
    }

    /**
     * unlock
     *
     * @param param
     * @return
     * @throws Exception
     */
    @RequestMapping("/unlock")
    public MQProxyResponse<String> unlock(@Valid ConsumeParam param, HttpServletRequest request) throws Exception {
        getRequestIdFromCookie(param, request);
        FetchRequest fetchRequest = FetchRequest.build(param);
        // 获取消费代理
        ConsumerProxy consumer = consumerManager.getConsumer(fetchRequest);
        // offset ack
        return consumer.unlock(fetchRequest);
    }

    /**
     * 查询集群队列offset
     *
     * @param consumer
     * @return
     * @throws Exception
     */
    @GetMapping("/clustering/queue/offset")
    public MQProxyResponse<?> clusteringQueueOffset(@RequestParam("consumer") String consumer) throws Exception {
        return consumerManager.getConsumerQueueOffsetList(consumer);
    }

    /**
     * 查询广播队列offset
     *
     * @param consumer
     * @return
     * @throws Exception
     */
    @GetMapping("/broadcast/queue/offset")
    public MQProxyResponse<?> broadcastQueueOffset(@RequestParam("consumer") String consumer) throws Exception {
        return consumerManager.getConsumerQueueOffsetList(consumer);
    }

    /**
     * 解注册
     *
     * @param consumer
     * @return
     * @throws Exception
     */
    @PostMapping("/unregister")
    public MQProxyResponse<?> unregister(ServletRequest request, @RequestParam("consumer") String consumer) throws Exception {
        log.info("user:{} unregister:{}", WebUtil.getUserFromAttribute(request), consumer);
        return consumerManager.unregister(consumer);
    }

    /**
     * 消费者配置
     *
     * @param consumerConfigParam
     * @return
     * @throws Exception
     */
    @PostMapping("/consumer/config")
    public MQProxyResponse<?> consumerConfig(ServletRequest request,
                                             @RequestBody @Valid ConsumerConfigParam consumerConfigParam) throws Exception {
        log.info("user:{} consumerConfig:{}", WebUtil.getUserFromAttribute(request), consumerConfigParam);
        consumerConfigManager.updateConsumerConfig(consumerConfigParam);
        return MQProxyResponse.buildOKResponse();
    }

    /**
     * 查询消费者配置
     *
     * @param consumer
     * @return
     * @throws Exception
     */
    @GetMapping("/config/{consumer}")
    public MQProxyResponse<?> consumerConfig(@PathVariable("consumer") String consumer) throws Exception {
        return MQProxyResponse.buildOKResponse(consumerManager.getConsumer(consumer));
    }
}
