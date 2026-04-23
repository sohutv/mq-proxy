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
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.sohu.tv.mq.proxy.model.MQProxyResponse.ERROR_STATUS;
import static com.sohu.tv.mq.proxy.model.MQProxyResponse.OK_STATUS;

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
     *
     * @param topicProducer
     */
    public boolean register(TopicProducer topicProducer) {
        String producer = topicProducer.getProducer();
        String topic = topicProducer.getTopic();
        ProducerProxy producerProxy = producerMap.get(producer);
        if (producerProxy != null) {
            log.error("producer:{} have registered:{}", producer, topic);
            return false;
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
            return true;
        } else {
            log.info("producer:{} register:{} failed, start failed", producer, topic);
            return false;
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
     *
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
         *
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
            if (param.isRoundRobin()) {
                return sendOrderMessage(param);
            }
            // 发送普通消息
            return sendCommonMessage(param);
        }

        /**
         * 发送普通消息
         */
        public Result<SendResult> sendCommonMessage(MessageParam param) {
            MQMessage mqMessage = MQMessage.build(param.getMessage()).setKeys(param.getKeys()).setClientHost(param.getClientIp());
            if (param.getDelayLevel() != null) {
                mqMessage.setDelayTimeLevel(param.getDelayLevel());
            } else if (param.getDeliveryTimestamp() != null) {
                mqMessage.setDeliveryTimestamp(param.getDeliveryTimestamp());
            }
            // 设置重试次数
            if (param.getAsyncRetryTimesIfSendFailed() == null) {
                mqMessage.setRetryTimes(0);
            } else {
                mqMessage.setRetryTimes(param.getAsyncRetryTimesIfSendFailed());
            }
            addLiteTopics(mqMessage, param.getLiteTopics());
            return producer.send(mqMessage);
        }

        private void addLiteTopics(MQMessage mqMessage, String liteTopics) {
            if (liteTopics == null) {
                return;
            }
            String[] liteTopicArray = liteTopics.split(",");
            for (String liteTopic : liteTopicArray) {
                liteTopic = liteTopic.trim();
                if (liteTopic.length() > 0) {
                    mqMessage.addLiteTopic(liteTopic);
                }
            }
        }

        /**
         * 发送有序消息
         */
        public Result<SendResult> sendOrderIdMessage(MessageParam param) {
            Long num = Long.valueOf(param.getOrderId().hashCode());
            return sendOrderMessage(param, num);
        }

        /**
         * 发送有序消息
         */
        public Result<SendResult> sendOrderMessage(MessageParam param) {
            return sendOrderMessage(param, getOrderNum());
        }

        public Result<SendResult> sendOrderMessage(MessageParam param, Long orderArg) {
            MQMessage mqMessage = MQMessage.build(param.getMessage()).setKeys(param.getKeys())
                    .setClientHost(param.getClientIp()).setOrderArg(orderArg);
            addLiteTopics(mqMessage, param.getLiteTopics());
            return producer.send(mqMessage);
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

        /**
         * 发送批量消息
         */
        public MQProxyResponse<?> send(List<String> messages, String clientHost) {
            return sendBatchMessage(buildMessageList(messages, clientHost));
        }

        /**
         * 构建消息列表
         */
        public List<MQProxyResponse> buildMessageList(List<String> messages, String clientHost) {
            return messages.stream().map(s -> buildMessage(s, clientHost)).collect(Collectors.toList());
        }

        /**
         * 构建消息
         */
        private MQProxyResponse buildMessage(String message, String clientHost) {
            try {
                return MQProxyResponse.buildOKResponse(producer.buildMessage(message, clientHost));
            } catch (Exception e) {
                log.error("build message:{}", message, e);
                return MQProxyResponse.buildParamErrorResponse("build message error:" + e.toString());
            }
        }

        /**
         * 发送消息
         */
        public MQProxyResponse<?> sendBatchMessage(List<MQProxyResponse> messageList) {
            long batchMessageLength = messageList.stream().filter(MQProxyResponse::ok).mapToLong(r -> {
                Message message = (Message) r.getResult();
                return message.getBody().length;
            }).sum();
            if (batchMessageLength == 0) {
                return MQProxyResponse.buildParamErrorResponse("no valid messages", messageList);
            }
            // 总大小超过最大值，使用异步发送
            if (batchMessageLength > producer.getProducer().getMaxMessageSize()) {
                return asyncSend(messageList);
            }
            return batchSend(messageList);
        }

        /**
         * 批量发送
         */
        public MQProxyResponse<?> batchSend(List<MQProxyResponse> messageList) {
            List<Message> messages = messageList.stream()
                    .filter(MQProxyResponse::ok)
                    .map(r -> (Message) r.removeResult())
                    .collect(Collectors.toList());
            Result<SendResult> result = producer.publish(messages);
            return processResponse(messageList, result.getException());
        }

        /**
         * 异步发送
         */
        public MQProxyResponse<?> asyncSend(List<MQProxyResponse> messageList) {
            List<CompletableFuture> futureList = messageList.stream()
                    .filter(MQProxyResponse::ok)
                    .map(this::asyncSend)
                    .collect(Collectors.toList());
            CompletableFuture<Void> allOf = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]));
            Exception exception = null;
            try {
                allOf.get(producer.getSendMsgTimeout() + 1000, TimeUnit.SECONDS);
            } catch (Exception e) {
                exception = e;
            }
            return processResponse(messageList, exception);
        }

        /**
         * 处理响应
         */
        private MQProxyResponse processResponse(List<MQProxyResponse> messageList, Exception e) {
            return messageList.stream()
                    .filter(response -> !response.ok())
                    .findAny()
                    .map(r -> buildErrorResponse(messageList, e))
                    .orElseGet(MQProxyResponse::buildOKResponse);
        }

        /**
         * 构建错误响应
         */
        private MQProxyResponse buildErrorResponse(List<MQProxyResponse> messageList, Exception e) {
            String errorMessage = "some messages failed to send";
            if (e != null) {
                log.error("send error", e);
                errorMessage = e.toString();
            }
            return MQProxyResponse.buildErrorResponse(errorMessage, messageList);
        }

        /**
         * 异步发送
         */
        public CompletableFuture asyncSend(MQProxyResponse response) {
            Message message = (Message) response.removeResult();
            response.setStatus(ERROR_STATUS);
            CompletableFuture<Void> future = new CompletableFuture<>();
            producer.publishAsync(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    response.setStatus(OK_STATUS);
                    future.complete(null);
                }

                @Override
                public void onException(Throwable throwable) {
                    log.error("send message:{} error", message, throwable);
                    response.setMessage(throwable.toString());
                    future.complete(null);
                }
            });
            return future;
        }
    }
}
