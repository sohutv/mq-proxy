package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sohu.index.tv.mq.common.PullResponse;
import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffset;
import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffsetResult;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.TopicConsumer;
import com.sohu.tv.mq.proxy.consumer.offset.ConsumerQueueOffsetManager;
import com.sohu.tv.mq.proxy.consumer.offset.EmptyOffsetStore;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.store.IRedis;
import com.sohu.tv.mq.rocketmq.RocketMQPullConsumer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
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

    public static final String SCHEDULE_UPDATE_NEWER_MAX_OFFSET_TOPIC = "sunmot";

    @Autowired
    private IRedis redis;

    @Value("${mqcloud.domain}")
    private String mqcloudDomain;

    private ConcurrentMap<String, ConsumerProxy> consumerMap = new ConcurrentHashMap<>();

    // 消息队列变更补偿线程池
    private ScheduledExecutorService messageQueueChangedExecutorService;

    // 较新的最大偏移量更新线程池
    private ThreadPoolExecutor newerMaxOffsetUpdateExecutorService;

    // topic级别较新的最大偏移量更新线程池
    private ScheduledExecutorService topicNewerMaxOffsetUpdateExecutorService;

    public ConsumerManager() {
        messageQueueChangedExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("consumerManagerThread").setDaemon(true).build());

        newerMaxOffsetUpdateExecutorService = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors() * 2,
                10, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(100),
                new ThreadFactoryBuilder().setNameFormat("newerMaxOffsetUpdate-%d").setDaemon(true).build());

        topicNewerMaxOffsetUpdateExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("topicNewerMaxOffsetUpdateThread").setDaemon(true).build());
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
        messageQueueChangedExecutorService.scheduleAtFixedRate(() -> {
            try {
                messageQueueChanged();
            } catch (Throwable e) {
                log.error("messageQueueChanged error", e);
            }
        }, 2, 5, TimeUnit.MINUTES);

        // 启动topic级别较新的最大偏移量更新任务
        topicNewerMaxOffsetUpdateExecutorService.scheduleAtFixedRate(() -> {
            try {
                updateNewerMaxOffset();
            } catch (Throwable e) {
                log.error("updateNewerMaxOffset error", e);
            }
        }, 5, 30, TimeUnit.MINUTES);
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
     * 更新较新的最大偏移量
     */
    public void updateNewerMaxOffset(List<ConsumerQueueOffset> consumerQueueOffsets, ConsumerProxy consumerProxy) {
        try {
            newerMaxOffsetUpdateExecutorService.execute(() -> {
                try {
                    consumerProxy.updateNewerMaxOffset(consumerQueueOffsets, consumerMap.values());
                } catch (Throwable e) {
                    log.error("updateNewerMaxOffset:{} error", consumerProxy.getGroup(), e);
                }
            });
        } catch (RejectedExecutionException e) {
            log.error("updateNewerMaxOffset:{} rejected, activeCount:{}, queueSize:{}", consumerProxy.getGroup(),
                    newerMaxOffsetUpdateExecutorService.getActiveCount(),
                    newerMaxOffsetUpdateExecutorService.getQueue().size(), e);
        }
    }

    /**
     * 更新较新的最大偏移量
     */
    public void updateNewerMaxOffset() {
        String value = redis.get(SCHEDULE_UPDATE_NEWER_MAX_OFFSET_TOPIC);
        if (StringUtils.isEmpty(value)) {
            return;
        }
        long start = System.currentTimeMillis();
        String[] topics = value.split(",");
        for (String topic : topics) {
            updateNewerMaxOffset(topic);
        }
        log.info("updateNewerMaxOffset size:{} cost:{}", topics.length, System.currentTimeMillis() - start);
    }

    /**
     * 更新topic较新的最大偏移量
     */
    public void updateNewerMaxOffset(String topic) {
        List<ConsumerProxy> consumerProxies = new ArrayList<>();
        for(Entry<String, ConsumerProxy> entry : consumerMap.entrySet()) {
            if(entry.getValue().getTopic().equals(topic)) {
                consumerProxies.add(entry.getValue());
            }
        }
        if (consumerProxies.isEmpty()) {
            log.warn("updateNewerMaxOffset:{} consumer is empty", topic);
            return;
        }
        consumerProxies.get(0).updateNewerMaxOffset(consumerProxies);
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

        public void updateMaxOffset(FetchRequest request, ConsumerQueueOffset consumerQueueOffset, long maxOffset) {
            getQueueOffsetManager().updateMaxOffset(request, consumerQueueOffset, maxOffset);
        }

        public void updateNewerMaxOffset(TopicStatsTable topicStatsTable) {
            getQueueOffsetManager().updateNewerMaxOffset(topicStatsTable);
        }

        public boolean unlock(String clientId, MessageQueue mq) {
            try {
                return getQueueOffsetManager().unlockByClientId(clientId, mq);
            } catch (Throwable e) {
                log.error("unlock {}:{}", consumer.getConsumer(), mq, e);
            }
            return false;
        }

        public MQProxyResponse<String> offsetAck(FetchRequest fetchRequest) {
            return getQueueOffsetManager().updateCommittedOffset(fetchRequest);
        }

        public MQProxyResponse<String> unlock(FetchRequest fetchRequest) {
            return getQueueOffsetManager().unlock(fetchRequest);
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

        public ConsumerQueueOffsetResult choose(String clientId) throws MQClientException {
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

        public void updateNewerMaxOffset(List<ConsumerQueueOffset> consumerQueueOffsets, Collection<ConsumerProxy> consumerMap) {
            getQueueOffsetManager().updateNewerMaxOffset(consumerQueueOffsets, consumerMap);
        }

        public void updateNewerMaxOffset(List<ConsumerProxy> consumerProxies) {
            getQueueOffsetManager().updateNewerMaxOffset(consumerProxies);
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

        public RocketMQPullConsumer consumer() {
            return consumer;
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
