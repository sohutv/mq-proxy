package com.sohu.tv.mq.proxy.consumer.offset;

import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffset;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ClientId;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.store.IRedis;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.params.SetParams;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * 消费队列偏移量管理
 *
 * @author: yongfeigao
 * @date: 2022/5/31 15:25
 */
public class ConsumerQueueOffsetManager implements MessageQueueListener {
    public static final String SPLITER = ":";
    public static final String COMMITTED_OFFSET_FIELD = "cmt";
    public static final String MAX_OFFSET_FIELD = "mx";
    public static final String LOCK_FIELD = "l";
    public static final String LAST_CONSUME_TIMESTAMP_FIELD = "t";
    // 广播模式客户端过期
    private long broadcastClientExpire = 10 * 60 * 1000L;

    private Logger log;

    private ConsumerProxy consumerProxy;
    // jedis
    private IRedis redis;

    public ConsumerQueueOffsetManager(ConsumerProxy consumerProxy, IRedis redis) {
        this.consumerProxy = consumerProxy;
        this.redis = redis;
        log = LoggerFactory.getLogger(consumerProxy.getGroup());
    }

    /**
     * 挑选合适的ConsumerQueueOffset
     *
     * @return ConsumerQueueOffset
     */
    public ConsumerQueueOffset choose(String clientId) throws MQClientException {
        // 获取所有的队列偏移量
        String key = toKey(clientId);
        Map<String, String> map = redis.hgetAll(key);
        if (map == null) {
            return null;
        }
        // 获取订阅的mq
        Set<MessageQueue> mqSet = consumerProxy.fetchSubscribeMessageQueues();
        // 转化队列偏移量为对象
        List<ConsumerQueueOffset> consumerQueueOffsetList = toConsumerQueueOffsetList(map);
        for (ConsumerQueueOffset consumerQueueOffset : consumerQueueOffsetList) {
            ConsumerQueueOffset chosen = choose(key, mqSet, consumerQueueOffset);
            if (chosen != null) {
                return chosen;
            }
        }
        return null;
    }

    /**
     * 选择合适的队列
     *
     * @param now
     * @param mqSet
     * @param queueOffset
     * @return
     */
    private ConsumerQueueOffset choose(String key, Set<MessageQueue> mqSet, ConsumerQueueOffset queueOffset) {
        // queue 有锁
        if (queueOffset.getLockTimestamp() > 0) {
            // 锁定超时了，删除锁
            if (isTimeout(queueOffset.getLockTimestamp())) {
                log.warn("expired lock:{} key:{}", queueOffset, key);
                unlock(key, queueOffset.getMessageQueue());
            } else {
                return null;
            }
        }
        // 队列非法
        if (!mqSet.contains(queueOffset.getMessageQueue())) {
            return null;
        }
        long lockTimestamp = System.currentTimeMillis();
        // 锁定队列失败
        if (!lock(key, queueOffset.getMessageQueue(), lockTimestamp)) {
            return null;
        }

        // 同步committedOffset
        syncCommittedOffset(key, queueOffset);
        queueOffset.setLockTimestamp(lockTimestamp);
        // 没有提交过offset，从最大偏移量消费
        if (queueOffset.getCommittedOffset() == -1) {
            long maxOffset = queueOffset.getMaxOffset();
            try {
                // 第一次消费，maxOffset可能存在很久没更新的情况，所以需要重新获取一下
                maxOffset = consumerProxy.maxOffset(queueOffset.getMessageQueue());
            } catch (Exception e) {
                log.error("choose:{} maxOffset error, use:{}", queueOffset.getMessageQueue(), maxOffset, e);
            }
            queueOffset.setCommittedOffset(maxOffset);
            queueOffset.setFirstConsume(true);
        }
        return queueOffset;
    }

    /**
     * 转化数据为对象
     *
     * @param map
     * @return
     */
    private List<ConsumerQueueOffset> toConsumerQueueOffsetList(Map<String, String> map) {
        Map<MessageQueue, ConsumerQueueOffset> consumerQueueOffsetMap = new HashMap<>();
        // 解析数据到map
        for (Map.Entry<String, String> entry : map.entrySet()) {
            // 0:brokerName 1:queueId 2:flag
            String[] keys = entry.getKey().split(SPLITER);
            MessageQueue messageQueue = new MessageQueue(consumerProxy.getTopic(), keys[0], Integer.valueOf(keys[1]));
            ConsumerQueueOffset consumerQueueOffset = consumerQueueOffsetMap.computeIfAbsent(messageQueue,
                    k -> new ConsumerQueueOffset(messageQueue));
            // 解析value
            long value = Long.valueOf(entry.getValue());
            switch (keys[2]) {
                case MAX_OFFSET_FIELD:
                    consumerQueueOffset.setMaxOffset(value);
                    break;
                case COMMITTED_OFFSET_FIELD:
                    consumerQueueOffset.setCommittedOffset(value);
                    break;
                case LOCK_FIELD:
                    consumerQueueOffset.setLockTimestamp(value);
                    break;
                case LAST_CONSUME_TIMESTAMP_FIELD:
                    consumerQueueOffset.setLastConsumeTimestamp(value);
                    break;
            }
        }
        // 按照消息量逆序；若消息量相同，按照消费时间从旧到新
        return consumerQueueOffsetMap.values().stream().filter(o -> o.valid()).sorted((o1, o2) -> {
            long count1 = o1.getMaxOffset() - o1.getCommittedOffset();
            long count2 = o2.getMaxOffset() - o2.getCommittedOffset();
            int rst = 0;
            // offset合法才按照消息量逆序
            if (count1 >= 0 && count2 >= 0) {
                rst = (int) (count2 - count1);
            }
            if (rst == 0) {
                // 消费时间从旧到新
                if (o1.getLastConsumeTimestamp() < o2.getLastConsumeTimestamp()) {
                    return -1;
                } else if (o1.getLastConsumeTimestamp() > o2.getLastConsumeTimestamp()) {
                    return 1;
                }
            }
            return rst;
        }).collect(Collectors.toList());
    }

    /**
     * 队列变更
     *
     * @param topic
     * @param mqAll
     * @param mqDivided
     */
    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        if (!consumerProxy.getTopic().equals(topic)) {
            log.warn("{} not equals param:{}", consumerProxy.getTopic(), topic);
            return;
        }
        if (consumerProxy.isBroadcasting()) {
            long now = System.currentTimeMillis();
            for (Entry<String, ClientId> entry : consumerProxy.getClientIdMap().entrySet()) {
                ClientId clientId = entry.getValue();
                // 超时未访问，则当做过期处理
                if (now - clientId.getAccessTime() > broadcastClientExpire) {
                    consumerProxy.getClientIdMap().remove(entry.getKey());
                    log.warn("consumer:{}:{} expired", consumerProxy.getGroup(), entry.getKey());
                } else {
                    updateQueueOffset(entry.getKey(), mqAll);
                }
            }
        } else {
            updateQueueOffset(null, mqAll);
        }
    }

    /**
     * 队列偏移量是否存在
     * @param clientId
     * @return
     */
    public boolean isQueueOffsetExist(String clientId) {
        String key = toKey(clientId);
        return redis.exists(key);
    }

    /**
     * 更新队列偏移量
     * @param clientId
     * @param mqAll
     * @return
     */
    public boolean updateQueueOffset(String clientId, Set<MessageQueue> mqAll) {
        // 加锁，保障锁定时间内只处理一次
        if (!lockQueueOffset(clientId)) {
            log.warn("lock {}:{} failed, maybe locked by other app", consumerProxy.getGroup(), clientId);
            return false;
        }
        boolean changed = false;
        try {
            String key = toKey(clientId);
            // 获取所有队列
            Map<String, String> map = redis.hgetAll(key);
            Set<String> fieldSet = new HashSet<>();
            // 1.mqAll中存在，map中无，新增
            for (MessageQueue mq : mqAll) {
                String field = toMaxOffsetField(mq);
                // 此队列为新增的
                if (map == null || !map.containsKey(field)) {
                    long maxOffset = consumerProxy.maxOffset(mq);
                    Long rst = redis.hsetnx(key, field, String.valueOf(maxOffset));
                    if (rst != null && 1 == rst) {
                        log.info("clientId:{} add {}:{}", clientId, field, maxOffset);
                    } else {
                        log.warn("clientId:{} add {} failed", clientId, field);
                    }
                    changed = true;
                }
                fieldSet.add(field);
                fieldSet.add(toCommittedOffsetField(mq));
                fieldSet.add(toLockField(mq));
                fieldSet.add(toLastConsumeTimestampField(mq));
            }
            // 2.map中有，mqAll中无，删除
            if (map != null) {
                for (String field : map.keySet()) {
                    if (!fieldSet.contains(field) && isMaxOffsetField(field)) {
                        Long rst = redis.hdel(key, field);
                        log.info("clientId:{} remove {} {}", clientId, field, rst);
                        changed = true;
                    }
                }
            }
            if (changed) {
                log.info("clientId:{} topic:{} changed from:{} to:{}", clientId, consumerProxy.getTopic(), map,
                        redis.hgetAll(key));
            }
        } catch (Exception e) {
            log.info("clientId:{} topic:{} queue:{} changed error", clientId, consumerProxy.getTopic(), mqAll, e);
        }
        return changed;
    }

    /**
     * 更新最大偏移量
     *
     * @param consumerQueueOffset
     * @param maxOffset
     */
    public void updateMaxOffset(String clientId, ConsumerQueueOffset consumerQueueOffset, long maxOffset) {
        String key = toKey(clientId);
        if (maxOffset > consumerQueueOffset.getMaxOffset()) {
            redis.hset(key, toMaxOffsetField(consumerQueueOffset.getMessageQueue()),
                    String.valueOf(maxOffset));
        }
        // 更新最近消费时间戳
        redis.hset(key, toLastConsumeTimestampField(consumerQueueOffset.getMessageQueue()),
                String.valueOf(System.currentTimeMillis()));
    }

    /**
     * 更新CommittedOffset
     *
     * @param fetchRequest
     */
    public void updateCommittedOffset(FetchRequest fetchRequest) {
        if (fetchRequest.isOffsetIllegal()) {
            return;
        }
        String key = toKey(fetchRequest.getClientId());
        String lockField = toField(fetchRequest.getBrokerName(), fetchRequest.getQueueId(), LOCK_FIELD);
        // 获取队列锁定时间
        String lockTimeValue = getLock(key, lockField);
        if (lockTimeValue == null) {
            // 锁不存在需要丢弃此次ack
            log.debug("unlocked, ignore request{}", fetchRequest);
            return;
        }
        long lockTime = NumberUtils.toLong(lockTimeValue);
        // 以下逻辑主要处理不用提交offset的情况
        if (!isTimeout(lockTime)) {
            // 时间不同，存在两种情况，1:该队列已经被其他实例消费 2:队列offset重复提交
            if (fetchRequest.getLockTime() < lockTime) {
                log.warn("ignore request:{} small than:{}", fetchRequest, lockTime);
                return;
            } else if (fetchRequest.getLockTime() > lockTime) {
                // 可能是bug，也可能是os时间不同步
                log.warn("maybe [bug] request:{} bigger than:{}", fetchRequest, lockTime);
            }
        } else {
            // 锁超时，但未被解锁，需要提交offset
            log.warn("lock expired:{}", fetchRequest);
        }
        // 获取提交的offset
        String committedOffsetField = toField(fetchRequest.getBrokerName(), fetchRequest.getQueueId(),
                COMMITTED_OFFSET_FIELD);
        String value = redis.hget(key, committedOffsetField);
        // 未提交过
        if (value == null || fetchRequest.isForceAck()) {
            redis.hset(key, committedOffsetField, String.valueOf(fetchRequest.getNextOffset()));
        } else {
            long committedOffset = NumberUtils.toLong(value);
            // 偏移量相等才提交
            if (fetchRequest.getOffset() == committedOffset) {
                if (fetchRequest.getNextOffset() > committedOffset) {
                    redis.hset(key, committedOffsetField, String.valueOf(fetchRequest.getNextOffset()));
                }
            } else {
                if (fetchRequest.getNextOffset() == committedOffset) {
                    log.warn("lockTime:{} offset has committed:{}", lockTimeValue, fetchRequest);
                } else {
                    log.error("maybe [bug] committedOffset:{} not equals:{}", committedOffset, fetchRequest);
                }
            }
        }
        // 解锁
        unlock(key, lockField);
    }

    private boolean isTimeout(long lockTime) {
        return System.currentTimeMillis() - lockTime > consumerProxy.getConsumeTimeoutInMillis();
    }

    /**
     * 获取 ConsumerQueueOffset list
     *
     * @return
     */
    public List<ConsumerQueueOffset> getClusteringQueueOffsets() {
        Map<String, String> map = redis.hgetAll(toKey(null));
        if (map == null) {
            return null;
        }
        // 转化数据
        return toConsumerQueueOffsetList(map);
    }

    public Map<String, List<ConsumerQueueOffset>> getBroadcastQueueOffsets() {
        Map<String, List<ConsumerQueueOffset>> resultMap = null;
        for (String clientId : consumerProxy.getClientIdMap().keySet()) {
            Map<String, String> map = redis.hgetAll(toKey(clientId));
            if (map == null) {
                continue;
            }
            if (resultMap == null) {
                resultMap = new HashMap<>();
            }
            resultMap.put(clientId, toConsumerQueueOffsetList(map));
        }
        return resultMap;
    }

    /**
     * 重置偏移量
     * @param clientId
     * @param timestamp
     * @throws MQClientException
     */
    public void resetOffset(String clientId, long timestamp) throws MQClientException {
        if (!consumerProxy.isBroadcasting()) {
            // 集群模式重置偏移量
            _resetOffset(null, timestamp);
        } else {
            Set<String> clientIdSet = consumerProxy.getClientIdMap().keySet();
            if (clientId != null && clientIdSet.contains(clientId)) {
                _resetOffset(clientId, timestamp);
            } else {
                // 重置所有实例偏移量
                for (String cid : clientIdSet) {
                    _resetOffset(cid, timestamp);
                }
            }
        }
    }

    /**
     * 重置偏移量
     * @param clientId
     * @param timestamp
     * @throws MQClientException
     */
    public void _resetOffset(String clientId, long timestamp) throws MQClientException {
        Set<MessageQueue> mqs = consumerProxy.fetchSubscribeMessageQueues();
        String key = toKey(clientId);
        for (MessageQueue mq : mqs) {
            long offset = consumerProxy.searchOffset(mq, timestamp);
            redis.hset(key, toCommittedOffsetField(mq), String.valueOf(offset));
            long maxOffset = consumerProxy.maxOffset(mq);
            redis.hset(key, toMaxOffsetField(mq), String.valueOf(maxOffset));
            log.info("clientId:{} consumer:{} mq:{} tm:{} resetOffset:{} maxOffset:{}", clientId, consumerProxy.getGroup(), mq,
                    timestamp, offset, maxOffset);
        }
    }

    private String getLock(String key, String lockField) {
        return redis.hget(key, lockField);
    }

    private boolean lock(String key, MessageQueue mq, long lockTimestamp) {
        Long rst = redis.hsetnx(key, toLockField(mq), String.valueOf(lockTimestamp));
        return rst != null && rst == 1;
    }

    public boolean unlockByClientId(String clientId, MessageQueue mq) {
        return unlock(toKey(clientId), toLockField(mq));
    }

    public boolean unlock(String key, MessageQueue mq) {
        return unlock(key, toLockField(mq));
    }

    private boolean unlock(String key, String lockField) {
        Long rst = redis.hdel(key, lockField);
        return rst != null && rst == 1;
    }

    private boolean lockQueueOffset(String clientId) {
        String lockKey;
        if (clientId == null) {
            lockKey = consumerProxy.getGroup() + SPLITER + LOCK_FIELD;
        } else {
            lockKey = consumerProxy.getGroup() + SPLITER + clientId + SPLITER + LOCK_FIELD;
        }
        // 60秒内只更新一次
        String rst = redis.set(lockKey, "L", SetParams.setParams().nx().ex(60));
        return "OK".equals(rst);
    }

    private String toCommittedOffsetField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), COMMITTED_OFFSET_FIELD);
    }

    private String toMaxOffsetField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), MAX_OFFSET_FIELD);
    }

    public boolean isMaxOffsetField(String field) {
        return field.endsWith(MAX_OFFSET_FIELD);
    }

    private String toLastConsumeTimestampField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), LAST_CONSUME_TIMESTAMP_FIELD);
    }

    private String toLockField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), LOCK_FIELD);
    }

    private String toField(String brokerName, int queueId, String flag) {
        return brokerName + SPLITER + queueId + SPLITER + flag;
    }

    private String toKey(String clientId) {
        if (clientId == null) {
            return consumerProxy.getGroup();
        }
        return consumerProxy.getGroup() + SPLITER + clientId;
    }

    private boolean isLocked(String value) {
        if (value.contains(SPLITER)) {
            return true;
        }
        return false;
    }

    /**
     * 同步CommittedOffset
     *
     * @param key
     * @param queueOffset
     */
    private void syncCommittedOffset(String key, ConsumerQueueOffset queueOffset) {
        String committedOffsetField = toField(queueOffset.getMessageQueue().getBrokerName(),
                queueOffset.getMessageQueue().getQueueId(), COMMITTED_OFFSET_FIELD);
        String value = redis.hget(key, committedOffsetField);
        if (value != null) {
            queueOffset.setCommittedOffset(Long.valueOf(value));
        }
    }
}
