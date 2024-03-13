package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.index.tv.mq.common.PullResponse;
import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffset;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.TopicConsumer;
import com.sohu.tv.mq.proxy.consumer.offset.ConsumerQueueOffsetManager;
import com.sohu.tv.mq.proxy.consumer.offset.EmptyOffsetStore;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.store.IRedis;
import com.sohu.tv.mq.proxy.util.ServiceLoadUtil;
import com.sohu.tv.mq.rocketmq.RocketMQProducer;
import com.sohu.tv.mq.rocketmq.RocketMQPullConsumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.*;

/**
 * 消费者管理：注册，变更等
 *
 * @author: yongfeigao
 * @date: 2022/6/7 10:56
 */
@Slf4j
@Component
public class ConsumerManager {
    @Autowired
    private IRedis redis;

    @Value("${mqcloud.domain}")
    private String mqcloudDomain;

    private ConcurrentMap<String, ConsumerProxy> consumerMap = new ConcurrentHashMap<>();

    private ScheduledExecutorService taskExecutorService;

    public ConsumerManager() {
        taskExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "consumerManagerThread");
            }
        });
        start();
    }

    /**
     * 注册消费者
     *
     * @param topic
     * @param consumerGroup
     * @return
     */
    public MQProxyResponse<?> register(TopicConsumer topicConsumer) {
        String consumerGroup = topicConsumer.getConsumer();
        String topic = topicConsumer.getTopic();
        ConsumerProxy consumerProxy = consumerMap.get(consumerGroup);
        if (consumerProxy != null) {
            log.error("consumer:{} have registered:{}", consumerGroup, topic);
            return MQProxyResponse.buildParamErrorResponse(consumerGroup + " have registered!");
        }
        // 创建PullConsumer
        RocketMQPullConsumer consumer = new RocketMQPullConsumer(consumerGroup, topic);
        consumer.setMqCloudDomain(mqcloudDomain);
        // 置空偏移量存储
        consumer.getConsumer().setOffsetStore(new EmptyOffsetStore());
        consumer.getConsumer().setEnableStreamRequestType(false);
        // 注册队列变更监听
        consumerProxy = new ConsumerProxy(consumer, topicConsumer.getMessageModel());
        ConsumerQueueOffsetManager manager = new ConsumerQueueOffsetManager(consumerProxy, redis);
        consumer.getConsumer().setMessageQueueListener(manager);
        consumer.start();
        if (consumer.isRunning()) {
            ConsumerProxy preConsumer = consumerMap.putIfAbsent(consumerGroup, consumerProxy);
            if (preConsumer != null) {
                consumer.shutdown();
                log.error("consumer:{} registered:{} failed, prev exist!", consumerGroup, topic);
            } else {
                log.info("consumer:{} register:{} ok", consumerGroup, topic);
            }
        }
        return MQProxyResponse.buildOKResponse();
    }

    /**
     * 解注册消费者
     *
     * @param topic
     * @param consumerGroup
     * @return
     */
    public MQProxyResponse<?> unregister(String consumerGroup) {
        ConsumerProxy preConsumer = consumerMap.remove(consumerGroup);
        if (preConsumer == null) {
            log.warn("consumer:{} haven't registered", consumerGroup);
            return MQProxyResponse.buildParamErrorResponse(consumerGroup + " haven't registered!");
        }
        preConsumer.shutdown();
        log.info("consumer:{} unregister:{} ok", consumerGroup, preConsumer.getTopic());
        return MQProxyResponse.buildOKResponse();
    }

    /**
     * 获取consumer
     *
     * @param fetchRequest
     * @return
     */
    public ConsumerProxy getConsumer(FetchRequest fetchRequest) {
        ConsumerProxy consumerProxy = consumerMap.get(fetchRequest.getConsumer());
        if (consumerProxy == null) {
            throw new MQException("consumer:" + fetchRequest.getConsumer() + " haven't registered");
        }
        if (!fetchRequest.getTopic().equals(consumerProxy.getTopic())) {
            throw new MQException("consumer:" + fetchRequest.getConsumer() + " subscribe:"
                    + consumerProxy.getTopic() + " but fetch:" + fetchRequest.getTopic());
        }
        if (consumerProxy.isBroadcasting()) {
            if (fetchRequest.getClientId() == null) {
                throw new MQException("consumer:" + fetchRequest.getConsumer() + " clientId is null");
            }
            // 注册clientId
            registerClientId(consumerProxy, fetchRequest.getClientId());
        } else if (fetchRequest.getClientId() != null) {
            throw new MQException("consumer:" + fetchRequest.getConsumer() + " no need clientId");
        }
        return consumerProxy;
    }

    public ConsumerProxy getConsumer(String consumer) {
        return consumerMap.get(consumer);
    }

    /**
     * 注册clientId
     *
     * @param consumerProxy
     * @param clientId
     */
    private void registerClientId(ConsumerProxy consumerProxy, String clientId) {
        // 注册clientId
        if (consumerProxy.registerClientId(clientId)
                && !consumerProxy.getQueueOffsetManager().isQueueOffsetExist(clientId)) { // 首次注册更新队列
            try {
                consumerProxy.getQueueOffsetManager().updateQueueOffset(clientId,
                        consumerProxy.fetchSubscribeMessageQueues());
            } catch (MQClientException e) {
                log.warn("{}:{} updateQueueOffset error", consumerProxy.getGroup(), clientId);
            }
        }
    }

    /**
     * 获取消费偏移量
     *
     * @param consumer
     * @return
     */
    public MQProxyResponse<?> getConsumerQueueOffsetList(String consumer) {
        ConsumerProxy consumerProxy = consumerMap.get(consumer);
        if (consumerProxy == null) {
            log.error("consumer:{} haven't registered", consumer);
            return MQProxyResponse.buildErrorResponse("consumer:" + consumer + " haven't registered");
        }
        if (consumerProxy.isBroadcasting()) {
            return MQProxyResponse.buildOKResponse(consumerProxy.getQueueOffsetManager().getBroadcastQueueOffsets());
        }
        return MQProxyResponse.buildOKResponse(consumerProxy.getQueueOffsetManager().getClusteringQueueOffsets());
    }

    /**
     * 启动
     */
    public void start() {
        // 启动队列变更补偿任务
        taskExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    messageQueueChanged();
                } catch (Throwable e) {
                    log.error("messageQueueChanged error", e);
                }
            }
        }, 2, 5, TimeUnit.MINUTES);
    }

    /**
     * 队列变更补偿定时任务
     */
    public void messageQueueChanged() {
        consumerMap.values().forEach(consumerProxy -> {
            try {
                consumerProxy.getQueueOffsetManager().messageQueueChanged(consumerProxy.getTopic(),
                        consumerProxy.fetchSubscribeMessageQueues(), null);
            } catch (MQClientException e) {
                log.error("topic:{} consumer:{} fetchSubscribeMessageQueues error",
                        consumerProxy.getTopic(),
                        consumerProxy.getGroup(), e);
            }
        });
    }

    /**
     * 消费代理
     */
    public static class ConsumerProxy {
        private MessageModel messageModel;
        private RocketMQPullConsumer consumer;
        // 广播模式有多个clientId
        private ConcurrentMap<String, ClientId> clientIdMap;

        public ConsumerProxy(RocketMQPullConsumer pullConsumer, MessageModel messageModel) {
            this.consumer = pullConsumer;
            this.messageModel = messageModel;
            if (MessageModel.BROADCASTING == messageModel) {
                this.clientIdMap = new ConcurrentHashMap<>();
            }
        }

        public Set<MessageQueue> fetchSubscribeMessageQueues() throws MQClientException {
            return consumer.getConsumer().fetchSubscribeMessageQueues(consumer.getTopic());
        }

        public long maxOffset(MessageQueue mq) throws MQClientException {
            return consumer.getConsumer().maxOffset(mq);
        }

        public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
            return consumer.getConsumer().searchOffset(mq, timestamp);
        }

        public void updateMaxOffset(String clientId, ConsumerQueueOffset consumerQueueOffset, long maxOffset) {
            getQueueOffsetManager().updateMaxOffset(clientId, consumerQueueOffset, maxOffset);
        }

        public boolean unlock(String clientId, MessageQueue mq) {
            try {
                return getQueueOffsetManager().unlockByClientId(clientId, mq);
            } catch (Throwable e) {
                log.error("unlock {}:{}", consumer.getConsumer(), mq, e);
            }
            return false;
        }

        public void offsetAck(FetchRequest fetchRequest) {
            getQueueOffsetManager().updateCommittedOffset(fetchRequest);
        }

        /**
         * 广播模式注册客户端id
         *
         * @param clientId
         * @return 第一次注册返回true
         */
        public boolean registerClientId(String clientId) {
            boolean firstRegister = false;
            ClientId obj = clientIdMap.get(clientId);
            if (obj == null) {
                obj = new ClientId(clientId);
                ClientId pre = clientIdMap.putIfAbsent(clientId, obj);
                // 首次注册
                if (pre == null) {
                    firstRegister = true;
                } else {
                    obj = pre;
                }
            }
            obj.setAccessTime(System.currentTimeMillis());
            return firstRegister;
        }

        public ConsumerQueueOffset choose(String clientId) throws MQClientException {
            return getQueueOffsetManager().choose(clientId);
        }

        public boolean isBroadcasting() {
            return MessageModel.BROADCASTING == messageModel;
        }

        public String deserialize(MessageExt me) throws Exception {
            return consumer.deserialize(me);
        }

        public PullResponse pull(MessageQueue mq, long offset)
                throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
            return consumer.pull(mq, offset);
        }

        public MessageExt viewMessage(String topic, String uniqKey) throws RemotingException, MQBrokerException,
                InterruptedException, MQClientException {
            return consumer.getConsumer().viewMessage(topic, uniqKey);
        }

        public void resetOffset(String clientId, long timestamp) throws MQClientException {
            getQueueOffsetManager().resetOffset(clientId, timestamp);
        }

        public void shutdown() {
            consumer.shutdown();
        }

        public MessageModel getMessageModel() {
            return messageModel;
        }

        public ConcurrentMap<String, ClientId> getClientIdMap() {
            return clientIdMap;
        }

        public String getTopic() {
            return consumer.getTopic();
        }

        public String getGroup() {
            return consumer.getGroup();
        }

        private ConsumerQueueOffsetManager getQueueOffsetManager() {
            return (ConsumerQueueOffsetManager) consumer.getConsumer().getMessageQueueListener();
        }

        public long getConsumerPullTimeoutMillis() {
            return consumer.getConsumer().getConsumerPullTimeoutMillis();
        }

        public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
            consumer.getConsumer().setConsumerPullTimeoutMillis(consumerPullTimeoutMillis);
        }

        public int getMaxPullSize() {
            return consumer.getMaxPullSize();
        }

        public void setMaxPullSize(int maxPullSize) {
            consumer.setMaxPullSize(maxPullSize);
        }

        public long getConsumeTimeoutInMillis() {
            return consumer.getConsumeTimeoutInMillis();
        }

        public void setConsumeTimeoutInMillis(long consumeTimeoutInMillis) {
            consumer.setConsumeTimeoutInMillis(consumeTimeoutInMillis);
        }

        public boolean isPause() {
            return consumer.isPause();
        }

        public void setPause(boolean pause) {
            consumer.setPause(pause);
        }

        public boolean isRateLimierEnabled() {
            return consumer.getRateLimiter().isEnabled();
        }

        public void setRateLimierEnabled(boolean enabled) {
            consumer.getRateLimiter().setEnabled(enabled);
        }

        public double getLimitRate(){
            return consumer.getRateLimiter().getQps();
        }

        public void setLimitRate(double limitRate){
            consumer.getRateLimiter().setRate(limitRate);
        }

        public Object getQueueOffsets() {
            if (isBroadcasting()) {
                return getQueueOffsetManager().getBroadcastQueueOffsets();
            }
            return getQueueOffsetManager().getClusteringQueueOffsets();
        }
    }

    @Data
    public static class ClientId {
        private String clientId;
        private long accessTime;

        public ClientId(String clientId) {
            this.clientId = clientId;
        }
    }
}
