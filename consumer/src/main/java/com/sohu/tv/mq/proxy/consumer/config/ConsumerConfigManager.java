package com.sohu.tv.mq.proxy.consumer.config;

import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.consumer.web.param.ConsumerConfigParam;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.store.IRedis;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * 消费者配置管理
 *
 * @author: yongfeigao
 * @date: 2022/6/24 15:24
 */
@Slf4j
@Order(2)
@Component
public class ConsumerConfigManager implements ApplicationRunner {
    public static final String SPLITER = ":";
    // 消费配置的key
    public static final String CONSUMER_CONFIG_KEY = "consumer_config";
    // 每批消息消最大拉取量 默认32
    public static final String MAX_PULL_SIZE_FIELD = "sz";
    // 每批消息消费超时毫秒 默认5 * 60 * 1000
    public static final String CONSUME_TIMEOUT_IN_MILLIS_FIELD = "cst";
    // 消息拉取超时毫秒 默认10 * 1000
    public static final String PULL_TIMEOUT_IN_MILLIS_FIELD = "pt";
    // 暂停
    public static final String PAUSE_FIELD = "p";
    // 限速启用
    public static final String RATELIMIT_ENABLED_FIELD = "rl";
    // 限速速率
    public static final String LIMITRATE_FIELD = "rt";
    // 重试消息id map
    private ConcurrentMap<String, Set<String>> retryMsgIdMap = new ConcurrentHashMap<>();

    @Autowired
    private IRedis redis;

    @Autowired
    private ConsumerManager consumerManager;

    private ScheduledExecutorService taskExecutorService;

    public ConsumerConfigManager() {
        taskExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "consumerConfigManagerThread");
            }
        });
        start();
    }

    /**
     * 启动
     */
    public void start() {
        // 启动消费者配置更新任务
        taskExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    updateConsumerConfig();
                } catch (Throwable e) {
                    log.error("updateConsumerConfig error", e);
                }
            }
        }, 70, 50, TimeUnit.SECONDS);
    }

    /**
     * 设置消费者配置
     *
     * @param config
     */
    public void updateConsumerConfig(ConsumerConfigParam config) throws MQClientException {
        if (config.getMaxPullSize() != null) {
            setMaxPullSize(config.getConsumer(), config.getMaxPullSize());
        }
        if (config.getConsumeTimeoutInMillis() != null) {
            setConsumeTimeoutInMillis(config.getConsumer(), config.getConsumeTimeoutInMillis());
        }
        if (config.getPullTimeoutInMillis() != null) {
            setPullTimeoutInMillis(config.getConsumer(), config.getPullTimeoutInMillis());
        }
        if (config.getPause() != null) {
            setPause(config.getConsumer(), config.getPause());
        }
        if (config.getResetOffsetTimestamp() != null) {
            ConsumerProxy consumerProxy = consumerManager.getConsumer(config.getConsumer());
            if (consumerProxy == null) {
                throw new MQException("consumer:" + config.getConsumer() + " haven't registered");
            }
            consumerProxy.resetOffset(config.getClientId(), config.getResetOffsetTimestamp());
        }
        if (config.getRateLimitEnabled() != null) {
            setRateLimitEnabled(config.getConsumer(), config.getRateLimitEnabled());
        }
        if (config.getLimitRate() != null) {
            setLimitRate(config.getConsumer(), config.getLimitRate());
        }
        if (config.getRetryMsgId() != null) {
            ConsumerProxy consumerProxy = consumerManager.getConsumer(config.getConsumer());
            if (consumerProxy == null) {
                throw new MQException("consumer:" + config.getConsumer() + " haven't registered");
            }
            updateRetryMsgId(consumerProxy, config.getRetryMsgId());
        }
    }

    /**
     * 每批消息消最大拉取量 默认32
     *
     * @param consumer
     * @param maxPullSize
     */
    public void setMaxPullSize(String consumer, int maxPullSize) {
        String value = String.valueOf(maxPullSize);
        redis.hset(CONSUMER_CONFIG_KEY, buildField(consumer, MAX_PULL_SIZE_FIELD), value);
        log.info("{} maxPullSize set to {}", consumer, maxPullSize);
    }

    /**
     * 每批消息消费超时毫秒 默认5 * 60 * 1000
     *
     * @param consumer
     * @param consumeTimeoutInMillis
     */
    public void setConsumeTimeoutInMillis(String consumer, long consumeTimeoutInMillis) {
        String value = String.valueOf(consumeTimeoutInMillis);
        redis.hset(CONSUMER_CONFIG_KEY, buildField(consumer, CONSUME_TIMEOUT_IN_MILLIS_FIELD), value);
        log.info("{} consumeTimeoutInMillis set to {}", consumer, consumeTimeoutInMillis);
    }

    /**
     * 消息拉取超时毫秒 默认10 * 1000
     *
     * @param consumer
     * @param pullTimeoutInMillis
     */
    public void setPullTimeoutInMillis(String consumer, long pullTimeoutInMillis) {
        String value = String.valueOf(pullTimeoutInMillis);
        redis.hset(CONSUMER_CONFIG_KEY, buildField(consumer, PULL_TIMEOUT_IN_MILLIS_FIELD), value);
        log.info("{} pullTimeoutInMillis set to {}", consumer, pullTimeoutInMillis);
    }

    /**
     * 暂停
     *
     * @param consumer
     * @param pause
     */
    public void setPause(String consumer, int pause) {
        String value = String.valueOf(pause);
        redis.hset(CONSUMER_CONFIG_KEY, buildField(consumer, PAUSE_FIELD), value);
        log.info("{} pause set to {}", consumer, pause);
    }

    /**
     * 设置启用限速
     *
     * @param consumer
     * @param rateLimitEnabled
     */
    public void setRateLimitEnabled(String consumer, int rateLimitEnabled) {
        String value = String.valueOf(rateLimitEnabled);
        redis.hset(CONSUMER_CONFIG_KEY, buildField(consumer, RATELIMIT_ENABLED_FIELD), value);
        log.info("{} rateLimitEnabled set to {}", consumer, rateLimitEnabled);
    }

    /**
     * 设置速率
     *
     * @param consumer
     * @param rateLimitEnabled
     */
    public void setLimitRate(String consumer, double limitRate) {
        String value = String.valueOf(limitRate);
        redis.hset(CONSUMER_CONFIG_KEY, buildField(consumer, LIMITRATE_FIELD), value);
        log.info("{} limitRate set to {}", consumer, limitRate);
    }

    private String buildField(String consumer, String flag) {
        return consumer + SPLITER + flag;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("updateConsumerConfig");
        updateConsumerConfig();
    }

    /**
     * 更新消费者配置
     */
    public void updateConsumerConfig() {
        Map<String, String> configMap = redis.hgetAll(CONSUMER_CONFIG_KEY);
        if (CollectionUtils.isEmpty(configMap)) {
            return;
        }
        for (Entry<String, String> entry : configMap.entrySet()) {
            // format: 0:consumer 1:field
            String[] keys = entry.getKey().split(SPLITER);
            ConsumerProxy consumerProxy = consumerManager.getConsumer(keys[0]);
            if (consumerProxy == null) {
                log.warn("consumer config:{} no consumer", entry.getKey());
                continue;
            }
            String value = entry.getValue();
            switch (keys[1]) {
                case MAX_PULL_SIZE_FIELD:
                    int maxPullSize = consumerProxy.getMaxPullSize();
                    int newMaxPullSize = Integer.parseInt(value);
                    if (maxPullSize != newMaxPullSize) {
                        consumerProxy.setMaxPullSize(newMaxPullSize);
                        log.info("{} maxPullSize {}->{}", consumerProxy.getGroup(), maxPullSize, value);
                    }
                    break;
                case CONSUME_TIMEOUT_IN_MILLIS_FIELD:
                    long consumeTimeoutInMillis = consumerProxy.getConsumeTimeoutInMillis();
                    long newConsumeTimeoutInMillis = Long.parseLong(value);
                    if (consumeTimeoutInMillis != newConsumeTimeoutInMillis) {
                        consumerProxy.setConsumeTimeoutInMillis(newConsumeTimeoutInMillis);
                        log.info("{} consumeTimeoutInMillis {}->{}", consumerProxy.getGroup(), consumeTimeoutInMillis
                                , value);
                    }
                    break;
                case PULL_TIMEOUT_IN_MILLIS_FIELD:
                    long consumerPullTimeoutMillis = consumerProxy.getConsumerPullTimeoutMillis();
                    long newConsumerPullTimeoutMillis = Long.parseLong(value);
                    if (consumerPullTimeoutMillis != newConsumerPullTimeoutMillis) {
                        consumerProxy.setConsumerPullTimeoutMillis(newConsumerPullTimeoutMillis);
                        log.info("{} consumerPullTimeoutMillis {}->{}", consumerProxy.getGroup(),
                                consumerPullTimeoutMillis, value);
                    }
                    break;
                case PAUSE_FIELD:
                    boolean pause = consumerProxy.isPause();
                    boolean newPause = "1".equals(value);
                    if (pause != newPause) {
                        consumerProxy.setPause(newPause);
                        log.info("{} pause {}->{}", consumerProxy.getGroup(), pause, newPause);
                    }
                    break;
                case RATELIMIT_ENABLED_FIELD:
                    boolean rateLimitEnabled = consumerProxy.isRateLimierEnabled();
                    boolean newRateLimitEnabled = "1".equals(value);
                    if (rateLimitEnabled != newRateLimitEnabled) {
                        consumerProxy.setRateLimierEnabled(newRateLimitEnabled);
                        log.info("{} rateLimitEnabled {}->{}", consumerProxy.getGroup(), rateLimitEnabled,
                                newRateLimitEnabled);
                    }
                    break;
                case LIMITRATE_FIELD:
                    double limitRate = consumerProxy.getLimitRate();
                    double newLimitRate = Double.parseDouble(value);
                    if (limitRate != newLimitRate) {
                        consumerProxy.setLimitRate(newLimitRate);
                        log.info("{} limitRate {}->{}", consumerProxy.getGroup(), limitRate, newLimitRate);
                    }
                    break;
            }
        }
    }

    /**
     * 更新重试消息id
     * 注意：并发更新不保障之前的重试消息被消费，简化设计
     *
     * @param consumerProxy
     * @param ids
     */
    public void updateRetryMsgId(ConsumerProxy consumerProxy, String retryMsgId) {
        if (!consumerProxy.isBroadcasting()) {
            retryMsgIdMap.computeIfAbsent(consumerProxy.getGroup(), k -> new ConcurrentSkipListSet<>()).add(retryMsgId);
            return;
        }
        for (String clientId : consumerProxy.getClientIdMap().keySet()) {
            String key = consumerProxy.getGroup() + ":" + clientId;
            retryMsgIdMap.computeIfAbsent(key, k -> new ConcurrentSkipListSet<>()).add(retryMsgId);
        }
    }

    /**
     * 获取重试消息id
     *
     * @param consumer
     * @param clientId
     * @return
     */
    public Set<String> getRetryMsgIds(String consumer, String clientId) {
        if (clientId == null) {
            return retryMsgIdMap.get(consumer);
        }
        String key = consumer + ":" + clientId;
        return retryMsgIdMap.get(key);
    }


    /**
     * 消费重试的消息id
     * @param consumer
     * @param clientId
     * @param function
     * @param <R>
     * @return
     */
    public <R> List<R> consumeRetryMsgIds(String consumer, String clientId, Function<String, R> function) {
        Set<String> msgIds = getRetryMsgIds(consumer, clientId);
        if (CollectionUtils.isEmpty(msgIds)) {
            return null;
        }
        List<R> list = new LinkedList<>();
        Iterator<String> iterator = msgIds.iterator();
        while (iterator.hasNext()) {
            String msgId = iterator.next();
            R r = function.apply(msgId);
            if (r != null) {
                list.add(r);
            }
            iterator.remove();
        }
        if (list.size() == 0) {
            return null;
        }
        return list;
    }
}
