package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sohu.index.tv.mq.common.PullResponse;
import com.sohu.tv.mq.proxy.consumer.model.*;
import com.sohu.tv.mq.proxy.consumer.offset.ConsumerQueueOffsetManager;
import com.sohu.tv.mq.proxy.consumer.offset.EmptyOffsetStore;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.store.IRedis;
import com.sohu.tv.mq.rocketmq.RocketMQLmqPullConsumer;
import com.sohu.tv.mq.rocketmq.RocketMQPullConsumer;
import com.sohu.tv.mq.util.CommonUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
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

    @Value("${lmq.consumer.expireAfterAccessAtMinute}")
    private int lmqConsumerExpireAfterAccessAtMinute;

    private ConcurrentMap<String, ConsumerProxy> consumerMap = new ConcurrentHashMap<>();

    // 轻量级消费者缓存
    private Cache<String, ConsumerProxy> liteConsumerCache;

    // 消息队列变更补偿线程池
    private ScheduledExecutorService messageQueueChangedExecutorService;

    // 较新的最大偏移量更新线程池
    private ThreadPoolExecutor newerMaxOffsetUpdateExecutorService;

    // topic级别较新的最大偏移量更新线程池
    private ScheduledExecutorService topicNewerMaxOffsetUpdateExecutorService;

    // 轻量级消费者缓存清理线程池
    private ScheduledExecutorService liteConsumerCacheCleanUpExecutorService;

    @PostConstruct
    public void init() {
        liteConsumerCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(lmqConsumerExpireAfterAccessAtMinute))
                .maximumSize(100000)
                .removalListener(this::removeLiteConsumerCache)
                .build();
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

        liteConsumerCacheCleanUpExecutorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("liteConsumerCacheCleanUpThread").setDaemon(true).build());
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
            return MQProxyResponse.buildOKResponse();
        }
        return MQProxyResponse.buildErrorResponse("consumer:" + consumerGroup + " register failed");
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
    public ConsumerProxy getConsumer(FetchRequest fetchRequest) throws ExecutionException {
        if (fetchRequest.getLiteTopic() != null) {
            return getLiteConsumer(fetchRequest);
        }
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
            consumerProxy.registerForBroadcasting(fetchRequest.getClientId());
        } else if (fetchRequest.getClientId() != null) {
            throw new MQException("consumer:" + fetchRequest.getConsumer() + " no need clientId");
        }
        return consumerProxy;
    }

    public ConsumerProxy getLiteConsumer(FetchRequest fetchRequest) throws ExecutionException {
        String consumer = CommonUtil.buildLmqConsumer(fetchRequest.getConsumer());
        ConsumerProxy consumerProxy = liteConsumerCache.get(consumer, () -> buildLiteConsumerProxy(fetchRequest));
        if (consumerProxy.isBroadcasting()) {
            consumerProxy.registerForBroadcasting(fetchRequest.getClientId());
        }
        // 延续过期时间
        liteConsumerCache.put(consumer, consumerProxy);
        return consumerProxy;
    }

    /**
     * 构建轻量级消费者代理
     */
    private ConsumerProxy buildLiteConsumerProxy(FetchRequest fetchRequest) {
        // 创建PullConsumer
        RocketMQPullConsumer consumer = new RocketMQLmqPullConsumer(fetchRequest.getConsumer(),
                fetchRequest.getTopic(), fetchRequest.getLiteTopic());
        consumer.setMqCloudDomain(mqcloudDomain);
        // 置空偏移量存储
        consumer.getConsumer().setOffsetStore(new EmptyOffsetStore());
        consumer.getConsumer().setEnableStreamRequestType(false);
        // 注册队列变更监听
        MessageModel messageModel = MessageModel.CLUSTERING;
        if (fetchRequest.getClientId() != null) {
            messageModel = MessageModel.BROADCASTING;
        }
        ConsumerProxy consumerProxy = new ConsumerProxy(consumer, messageModel);
        ConsumerQueueOffsetManager manager = new ConsumerQueueOffsetManager(consumerProxy, redis);
        consumer.getConsumer().setMessageQueueListener(manager);
        consumer.start();
        if (consumer.isRunning()) {
            log.info("lite consumer:{} register ok", fetchRequest);
        }
        consumerProxy.updateQueueOffsetForClustering();
        return consumerProxy;
    }

    private void removeLiteConsumerCache(RemovalNotification<String, ConsumerProxy> notification) {
        RemovalCause removalCause = notification.getCause();
        if (removalCause == RemovalCause.REPLACED) {
            return;
        }
        String consumer = notification.getKey();
        ConsumerProxy consumerProxy = notification.getValue();
        log.info("lite consumer:{} removed, cause:{}", consumer, removalCause);
        if (consumerProxy == null) {
            return;
        }
        consumerProxy.getQueueOffsetManager().deleteConsumerQueueOffset();
        consumerProxy.shutdown();
        log.info("lite consumer:{} shutdown ok", consumer);
    }

    public ConsumerProxy getConsumer(String consumer) {
        if (MixAll.isLmq(consumer)) {
            return liteConsumerCache.getIfPresent(consumer);
        }
        return consumerMap.get(consumer);
    }

    /**
     * 获取消费偏移量
     *
     * @param consumer
     * @return
     */
    public MQProxyResponse<?> getConsumerQueueOffsetList(String consumer) {
        ConsumerProxy consumerProxy = getConsumer(consumer);
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
     * 获取消费偏移量
     */
    public MQProxyResponse<?> getPopAckInfo(String consumer, String clientId, int page, int pageSize) {
        ConsumerProxy consumerProxy = getConsumer(consumer);
        if (consumerProxy == null) {
            log.error("consumer:{} haven't registered", consumer);
            return MQProxyResponse.buildErrorResponse("consumer:" + consumer + " haven't registered");
        }
        int start = (page - 1) * pageSize;
        int end = start + pageSize - 1;
        return MQProxyResponse.buildOKResponse(consumerProxy.getQueueOffsetManager().getPopAckInfo(clientId, start, end));
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
                updateNewerMaxOffsetForConfigTopic();
            } catch (Throwable e) {
                log.error("updateNewerMaxOffsetForConfigTopic error", e);
            }
        }, 5, 30, TimeUnit.MINUTES);

        // 启动轻量级消费者缓存清理任务
        liteConsumerCacheCleanUpExecutorService.scheduleAtFixedRate(() -> {
            try {
                liteConsumerCache.cleanUp();
            } catch (Throwable e) {
                log.error("liteConsumerCache cleanUp error", e);
            }
        }, 10, 10, TimeUnit.MINUTES);
    }

    /**
     * 队列变更补偿定时任务
     */
    public void messageQueueChanged() {
        consumerMap.values().forEach(this::messageQueueChanged);
        liteConsumerCache.asMap().values().forEach(this::messageQueueChanged);
    }

    public void messageQueueChanged(ConsumerProxy consumerProxy) {
        try {
            consumerProxy.getQueueOffsetManager().messageQueueChanged(consumerProxy.getTopic(),
                    consumerProxy.fetchSubscribeMessageQueues(), null);
        } catch (MQClientException e) {
            log.error("topic:{} consumer:{} fetchSubscribeMessageQueues error",
                    consumerProxy.getTopic(),
                    consumerProxy.getGroup(), e);
        }
    }

    /**
     * 更新较新的最大偏移量
     */
    public void updateNewerMaxOffset(List<ConsumerQueueOffset> consumerQueueOffsets, ConsumerProxy consumerProxy) {
        try {
            newerMaxOffsetUpdateExecutorService.execute(() -> {
                try {
                    Collection<ConsumerProxy> consumers = null;
                    if (MixAll.isLmq(consumerProxy.getTopic())) {
                        consumers = liteConsumerCache.asMap().values();
                    } else {
                        consumers = consumerMap.values();
                    }
                    consumerProxy.updateNewerMaxOffset(consumerQueueOffsets, consumers);
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
    public void updateNewerMaxOffsetForConfigTopic() {
        String value = redis.get(SCHEDULE_UPDATE_NEWER_MAX_OFFSET_TOPIC);
        if (StringUtils.isEmpty(value)) {
            return;
        }
        long start = System.currentTimeMillis();
        String[] topics = value.split(",");
        for (String topic : topics) {
            updateNewerMaxOffsetForTopic(topic);
        }
        log.info("updateNewerMaxOffsetForConfigTopic size:{} cost:{}", topics.length, System.currentTimeMillis() - start);
    }

    /**
     * 更新topic较新的最大偏移量
     */
    public void updateNewerMaxOffsetForTopic(String topic) {
        List<ConsumerProxy> consumerProxies = new ArrayList<>();
        Set<Entry<String, ConsumerProxy>> entrySet = null;
        if (MixAll.isLmq(topic)) {
            entrySet = liteConsumerCache.asMap().entrySet();
        } else {
            entrySet = consumerMap.entrySet();
        }
        for (Entry<String, ConsumerProxy> entry : entrySet) {
            if (entry.getValue().getTopic().equals(topic)) {
                consumerProxies.add(entry.getValue());
            }
        }
        if (consumerProxies.isEmpty()) {
            log.warn("updateNewerMaxOffsetForTopic:{} consumer is empty", topic);
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
        private boolean pop;

        public ConsumerProxy(RocketMQPullConsumer pullConsumer, MessageModel messageModel) {
            this.consumer = pullConsumer;
            this.messageModel = messageModel;
            if (MessageModel.BROADCASTING == messageModel) {
                this.clientIdMap = new ConcurrentHashMap<>();
            }
        }

        public void registerForBroadcasting(String clientId) {
            // 注册clientId
            if (isBroadcasting() && registerClientId(clientId)) {
                // 首次注册更新队列
                updateQueueOffsetIfNeeded(clientId);
            }
        }

        public void updateQueueOffsetForClustering() {
            if (isBroadcasting()) {
                return;
            }
            updateQueueOffsetIfNeeded(null);
        }

        public void updateQueueOffsetIfNeeded(String clientId) {
            boolean queueOffsetExist = getQueueOffsetManager().isQueueOffsetExist(clientId);
            if (queueOffsetExist) {
                return;
            }
            try {
                getQueueOffsetManager().updateQueueOffset(clientId, fetchSubscribeMessageQueues(), true);
            } catch (MQClientException e) {
                log.warn("{}:{} updateQueueOffset error", getGroup(), clientId);
            }
        }

        public Set<MessageQueue> fetchSubscribeMessageQueues() throws MQClientException {
            return consumer.fetchSubscribeMessageQueues();
        }

        public long maxOffset(MessageQueue mq) throws MQClientException {
            return consumer.getConsumer().maxOffset(mq);
        }

        public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
            return consumer.getConsumer().searchOffset(mq, timestamp);
        }

        public void updateMaxOffset(FetchRequest request, ConsumerQueueOffset consumerQueueOffset, PullResponse pullResponse) {
            getQueueOffsetManager().updateMaxOffset(request, consumerQueueOffset, pullResponse);
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

        public MQProxyResponse<String> commitPopAck(FetchRequest fetchRequest) {
            return getQueueOffsetManager().commitPopAck(fetchRequest);
        }

        public MQProxyResponse<String> commitAck(FetchRequest fetchRequest) {
            return getQueueOffsetManager().updateCommittedOffset(fetchRequest);
        }

        public MQProxyResponse<String> saveProcessingPopAck(FetchRequest fetchRequest) {
            fetchRequest.setConsumeTimeout(getConsumeTimeoutInMillis());
            return getQueueOffsetManager().saveProcessingPopAck(fetchRequest);
        }

        public MQProxyResponse<String> unlock(FetchRequest fetchRequest) {
            return getQueueOffsetManager().unlock(fetchRequest);
        }

        public FetchRequest getTimeoutPopAckInfo(FetchRequest fetchRequest) {
            fetchRequest.setConsumeTimeout(getConsumeTimeoutInMillis());
            return getQueueOffsetManager().getTimeoutPopAck(fetchRequest);
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

        public PullResponse pull(MessageQueue mq, long offset, String clientIp)
                throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
            return consumer.pull(mq, offset, clientIp);
        }

        public PullResponse pull(MessageQueue mq, long offset, int maxNums, String clientIp)
                throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
            return consumer.pull(mq, offset, maxNums, clientIp);
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

        public boolean isRateLimiterEnabled() {
            return consumer.getRateLimiter().isEnabled();
        }

        public void setRateLimiterEnabled(boolean enabled) {
            consumer.getRateLimiter().setEnabled(enabled);
        }

        public double getLimitRate(){
            return consumer.getRateLimiter().getQps();
        }

        public void setLimitRate(double limitRate){
            consumer.getRateLimiter().setRate(limitRate);
        }

        public boolean isPop() {
            return pop;
        }

        public void setPop(boolean pop) {
            this.pop = pop;
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
