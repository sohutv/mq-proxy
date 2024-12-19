package com.sohu.tv.mq.proxy.producer.rocketmq;

import com.sohu.index.tv.mq.common.MQMessage;
import com.sohu.index.tv.mq.common.Result;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.producer.model.TopicProducer;
import com.sohu.tv.mq.proxy.producer.web.param.MessageParam;
import com.sohu.tv.mq.proxy.store.IRedis;
import com.sohu.tv.mq.proxy.util.ServiceLoadUtil;
import com.sohu.tv.mq.rocketmq.RocketMQProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ServiceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 生产者管理
 *
 * @author: yongfeigao
 * @date: 2022/6/23 10:38
 */
@Slf4j
@Component
public class ProducerManager {

    @Autowired
    private IRedis redis;

    @Value("${mqcloud.domain}")
    private String mqcloudDomain;

    private ConcurrentMap<String, ProducerProxy> producerMap = new ConcurrentHashMap<>();

    public ProducerProxy getProducer(String producer) {
        ProducerProxy consumerProxy = producerMap.get(producer);
        if (consumerProxy == null) {
            throw new MQException("producer:" + producer + " haven't registered");
        }
        return consumerProxy;
    }

    /**
     * 注册TopicProducer
     * @param topicProducer
     */
    public void register(TopicProducer topicProducer) {
        String producer = topicProducer.getProducer();
        String topic = topicProducer.getTopic();
        ProducerProxy producerProxy = producerMap.get(producer);
        if (producerProxy != null) {
            log.error("producer:{} have registered:{}", producer, topic);
            return;
        }
        // 创建ProducerProxy
        producerProxy = new ProducerProxy(producer, topic);
        producerProxy.getProducer().setMqCloudDomain(mqcloudDomain);
        if (producerProxy.start()) {
            ProducerProxy preProducer = producerMap.putIfAbsent(producer, producerProxy);
            if (preProducer != null) {
                producerProxy.getProducer().shutdown();
                log.error("producer:{} registered:{} failed, prev exist!", producer, topic);
            } else {
                log.info("producer:{} register:{} ok", producer, topic);
            }
        } else {
            log.info("producer:{} register:{} failed, start failed", producer, topic);
        }
    }

    /**
     * 解注册生产者
     *
     * @param producer
     * @return
     */
    public MQProxyResponse<?> unregister(String producer) {
        ProducerProxy producerProxy = producerMap.remove(producer);
        if (producerProxy == null) {
            log.warn("producer:{} haven't registered", producer);
            return MQProxyResponse.buildParamErrorResponse(producer + " haven't registered!");
        }
        producerProxy.getProducer().shutdown();
        log.info("producer:{} unregister:{} ok", producer, producerProxy.getProducer().getTopic());
        return MQProxyResponse.buildOKResponse();
    }

    /**
     * 生产代理
     * @author: yongfeigao
     * @date: 2022/6/24 16:01
     */
    public class ProducerProxy {
        private Logger log;

        private RocketMQProducer producer;

        public ProducerProxy(String producerGroup, String topic) {
            log = LoggerFactory.getLogger(producerGroup);
            producer = ServiceLoadUtil.loadService(RocketMQProducer.class, RocketMQProducer.class);
            producer.construct(producerGroup, topic);
            producer.setMessageQueueSelector(new RoundRobinMessageQueueSelector());
            // 设置重复回调消费
            producer.setResendResultConsumer(result -> {
                if (result.isSuccess()) {
                    log.info("resend:{} message:{} ok", result.getMqMessage().getRetryTimes(),
                            result.getMqMessage().getMessage());
                } else {
                    log.warn("resend:{} message:{} failed:{}", result.getMqMessage().getRetryTimes(),
                            result.getMqMessage().getMessage(), result.getException().toString());
                }
            });
        }

        /**
         * 启动
         * @return
         */
        public boolean start() {
            producer.start();
            return ServiceState.RUNNING == producer.getProducer().getDefaultMQProducerImpl().getServiceState();
        }

        /**
         * 消息发送
         *
         * @param param
         * @return
         */
        public Result<SendResult> send(MessageParam param) {
            // 根据orderId发送有序消息
            if (!param.isOrderIdEmpty()) {
                return sendOrderIdMessage(param);
            }
            // 发送有序消息
            if (param.isOrdered()) {
                return sendOrderMessage(param);
            }
            // 发送普通消息
            return sendCommonMessage(param);
        }

        /**
         * 发送普通消息
         */
        public Result<SendResult> sendCommonMessage(MessageParam param) {
            MQMessage mqMessage = MQMessage.build(param.getMessage()).setKeys(param.getKeys());
            if (param.getDelayLevel() != null) {
                mqMessage.setDelayTimeLevel(param.getDelayLevel());
            }
            // 设置重试次数
            if (param.getAsyncRetryTimesIfSendFailed() == null) {
                mqMessage.setRetryTimes(0);
            } else {
                mqMessage.setRetryTimes(param.getAsyncRetryTimesIfSendFailed());
            }
            return producer.send(mqMessage);
        }

        /**
         * 发送有序消息
         */
        public Result<SendResult> sendOrderIdMessage(MessageParam param) {
            Long num = Long.valueOf(param.getOrderId().hashCode());
            return producer.publishOrder(param.getMessage(), param.getKeys(), num);
        }

        /**
         * 发送有序消息
         */
        public Result<SendResult> sendOrderMessage(MessageParam param) {
            return producer.publishOrder(param.getMessage(), param.getKeys(), getOrderNum());
        }

        private Long getOrderNum() {
            String key = "order:" + producer.getTopic();
            try {
                return getOrderNum(key);
            } catch (JedisDataException e) {
                // 处理incr溢出情况
                log.error("key:{} getOrderNum error", key, e);
                redis.set(key, "0");
            }
            return getOrderNum(key);
        }

        private Long getOrderNum(String key) {
            Long num = redis.incr(key);
            if (num >= Long.MAX_VALUE) {
                redis.set(key, "0");
            }
            return num;
        }

        public RocketMQProducer getProducer() {
            return producer;
        }
    }
}
