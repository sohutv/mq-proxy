package com.sohu.tv.mq.proxy.consumer.offset;

import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffset;
import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffsetResult;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ClientId;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.store.IRedis;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
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
    public static final String LAST_CONSUME_CLIENT_IP = "ip";
    // 较新的最大偏移量字段
    public static final String NEWER_MAX_OFFSET_FIELD = "nmx";
    // 较新的最大偏移量更新锁
    public static final String NEWER_MAX_OFFSET_LOCK = "nmxl";
    // 较新的最大偏移量更新锁过期时间
    public static final String NEWER_MAX_OFFSET_LOCK_TIME = "nmxlt";
    // 较新的最大偏移量定时更新锁
    public static final String SCHEDULE_NEWER_MAX_OFFSET_LOCK = "snmxl";
    // 较新的最大偏移量定时更新锁最大过期时间
    public static final int MAX_SCHEDULE_NEWER_MAX_OFFSET_LOCK_EXPIRE = 20 * 60;
    // 较新的最大偏移量更新锁最大过期时间
    public static final int MAX_NEWER_MAX_OFFSET_LOCK_EXPIRE = 10;
    // 较新的最大偏移量更新锁最小过期时间
    public static final int MIN_NEWER_MAX_OFFSET_LOCK_EXPIRE = 3;
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
     */
    public ConsumerQueueOffsetResult choose(String clientId) throws MQClientException {
        // 获取所有的队列偏移量
        String key = toKey(clientId);
        Map<String, String> map = redis.hgetAll(key);
        if (map == null) {
            return null;
        }
        ConsumerQueueOffsetResult result = new ConsumerQueueOffsetResult();
        // 获取订阅的mq
        Set<MessageQueue> mqSet = consumerProxy.fetchSubscribeMessageQueues();
        // 转化队列偏移量为对象
        List<ConsumerQueueOffset> consumerQueueOffsetList = toConsumerQueueOffsetList(map);
        result.setConsumerQueueOffsets(consumerQueueOffsetList);
        for (ConsumerQueueOffset consumerQueueOffset : consumerQueueOffsetList) {
            ConsumerQueueOffset chosen = choose(key, mqSet, consumerQueueOffset);
            if (chosen != null) {
                result.setChosenConsumerQueueOffset(chosen);
                break;
            }
        }
        return result;
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
            String value = entry.getValue();
            switch (keys[2]) {
                case MAX_OFFSET_FIELD:
                    consumerQueueOffset.setMaxOffset(toLong(value));
                    break;
                case COMMITTED_OFFSET_FIELD:
                    consumerQueueOffset.setCommittedOffset(toLong(value));
                    break;
                case LOCK_FIELD:
                    consumerQueueOffset.setLockTimestamp(toLong(value));
                    break;
                case LAST_CONSUME_TIMESTAMP_FIELD:
                    consumerQueueOffset.setLastConsumeTimestamp(toLong(value));
                    break;
                case NEWER_MAX_OFFSET_FIELD:
                    consumerQueueOffset.setNewerMaxOffset(toLong(value));
                    break;
                case LAST_CONSUME_CLIENT_IP:
                    consumerQueueOffset.setLastConsumeClientIp(value);
                    break;
            }
        }
        // 按照消息量逆序；若消息量相同，按照消费时间从旧到新
        return consumerQueueOffsetMap.values().stream().filter(o -> o.valid()).sorted((o1, o2) -> {
            long count1 = o1.chooseMaxOffset() - o1.getCommittedOffset();
            long count2 = o2.chooseMaxOffset() - o2.getCommittedOffset();
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

    private long toLong(String value) {
        return Long.valueOf(value);
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
                    log.warn("{} expired", entry.getKey());
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
     *
     * @param clientId
     * @return
     */
    public boolean isQueueOffsetExist(String clientId) {
        String key = toKey(clientId);
        return redis.exists(key);
    }

    /**
     * 更新队列偏移量
     *
     * @param clientId
     * @param mqAll
     * @return
     */
    public boolean updateQueueOffset(String clientId, Set<MessageQueue> mqAll) {
        // 加锁，保障锁定时间内只处理一次
        if (!lockQueueOffset(clientId)) {
            log.warn("lock {} failed, maybe locked by other app", clientId);
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
                fieldSet.add(toLastConsumeClientIpField(mq));
            }
            // 2.map中有，mqAll中无，删除
            if (map != null) {
                for (String field : map.keySet()) {
                    // 这里只删除最大偏移量，怕broker下线后又上线，commitOffset丢失
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
     */
    public void updateMaxOffset(FetchRequest request, ConsumerQueueOffset consumerQueueOffset, long maxOffset) {
        String key = toKey(request.getClientId());
        if (maxOffset > consumerQueueOffset.getMaxOffset()) {
            redis.hset(key, toMaxOffsetField(consumerQueueOffset.getMessageQueue()),
                    String.valueOf(maxOffset));
        }
        // 更新最近消费时间戳
        redis.hset(key, toLastConsumeTimestampField(consumerQueueOffset.getMessageQueue()),
                String.valueOf(System.currentTimeMillis()));
        // 更新最近消费客户端ip
        if (request.getClientIp() != null) {
            redis.hset(key, toLastConsumeClientIpField(consumerQueueOffset.getMessageQueue()), request.getClientIp());
        }
    }

    /**
     * 更新CommittedOffset
     *
     * @param fetchRequest
     */
    public MQProxyResponse<String> updateCommittedOffset(FetchRequest fetchRequest) {
        String ackInfo = fetchRequest.getAckInfo();
        MQProxyResponse response = MQProxyResponse.buildOKResponse();
        if (fetchRequest.isOffsetIllegal()) {
            response.setMessage("ack illegal " + ackInfo);
            response.setStatus(400);
            return response;
        }
        String key = toKey(fetchRequest.getClientId());
        String lockField = toField(fetchRequest.getBrokerName(), fetchRequest.getQueueId(), LOCK_FIELD);
        // 获取队列锁定时间
        String lockTimeValue = getLock(key, lockField);
        if (lockTimeValue == null) {
            // 锁不存在需要丢弃此次ack
            log.debug("unlocked, ignore request{}", fetchRequest);
            response.setMessage("lock not exist " + ackInfo);
            return response;
        }
        long lockTime = NumberUtils.toLong(lockTimeValue);
        // 以下逻辑主要处理不用提交offset的情况
        if (!isTimeout(lockTime)) {
            // 时间不同，存在两种情况，1:该队列已经被其他实例消费 2:队列offset重复提交
            if (fetchRequest.getLockTime() < lockTime) {
                log.warn("ignore request:{} small than:{}", fetchRequest, lockTime);
                response.setMessage("ack lockTime small than " + lockTime + " " + ackInfo);
                return response;
            } else if (fetchRequest.getLockTime() > lockTime) {
                // 可能是bug，也可能是os时间不同步
                log.warn("maybe [bug] request:{} bigger than:{}", fetchRequest, lockTime);
                response.setMessage("bug ack lockTime bigger than " + lockTime + " " + ackInfo);
            }
        } else {
            // 锁超时，但未被解锁，需要提交offset
            log.warn("lock expired:{}", fetchRequest);
            response.setMessage("lock expired " + ackInfo);
        }
        // 获取提交的offset
        String committedOffsetField = toField(fetchRequest.getBrokerName(), fetchRequest.getQueueId(),
                COMMITTED_OFFSET_FIELD);
        String value = redis.hget(key, committedOffsetField);
        // 未提交过
        if (value == null || fetchRequest.isForceAck()) {
            redis.hset(key, committedOffsetField, String.valueOf(fetchRequest.getNextOffset()));
            response.setMessage("ok " + ackInfo);
        } else {
            long committedOffset = NumberUtils.toLong(value);
            // 偏移量相等才提交
            if (fetchRequest.getOffset() == committedOffset) {
                if (fetchRequest.getNextOffset() > committedOffset) {
                    redis.hset(key, committedOffsetField, String.valueOf(fetchRequest.getNextOffset()));
                    response.setMessage("ok " + ackInfo);
                }
            } else {
                if (fetchRequest.getNextOffset() == committedOffset) {
                    log.warn("lockTime:{} offset has committed:{}", lockTimeValue, fetchRequest);
                    response.setMessage("offset has committed " + ackInfo);
                } else {
                    log.error("maybe [bug] committedOffset:{} not equals:{}", committedOffset, fetchRequest);
                    response.setMessage("bug committedOffset not equal " + committedOffset + " " + ackInfo);
                }
            }
        }
        // 解锁
        unlock(key, lockField);
        return response;
    }

    /**
     * 解锁队列
     *
     * @param fetchRequest
     * @return
     */
    public MQProxyResponse<String> unlock(FetchRequest fetchRequest) {
        String ackInfo = fetchRequest.getAckInfo();
        MQProxyResponse response = MQProxyResponse.buildOKResponse();
        if (fetchRequest.isOffsetIllegal()) {
            response.setMessage("ack illegal " + ackInfo);
            response.setStatus(400);
            return response;
        }
        String key = toKey(fetchRequest.getClientId());
        String lockField = toField(fetchRequest.getBrokerName(), fetchRequest.getQueueId(), LOCK_FIELD);
        boolean ok = unlock(key, lockField);
        response.setMessage(ackInfo + " unlock:" + ok);
        return response;
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
     *
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
     *
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
            log.info("clientId:{} mq:{} tm:{} resetOffset:{} maxOffset:{}", clientId, mq, timestamp, offset, maxOffset);
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

    private String toNewerMaxOffsetField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), NEWER_MAX_OFFSET_FIELD);
    }

    public boolean isMaxOffsetField(String field) {
        return field.endsWith(SPLITER + MAX_OFFSET_FIELD);
    }

    private String toLastConsumeTimestampField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), LAST_CONSUME_TIMESTAMP_FIELD);
    }

    private String toLastConsumeClientIpField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), LAST_CONSUME_CLIENT_IP);
    }

    private String toLockField(MessageQueue mq) {
        return toField(mq.getBrokerName(), mq.getQueueId(), LOCK_FIELD);
    }

    private String toField(String brokerName, int queueId, String flag) {
        return brokerName + SPLITER + queueId + SPLITER + flag;
    }

    private String toNewerMaxOffsetLockKey() {
        return NEWER_MAX_OFFSET_LOCK + SPLITER + consumerProxy.getTopic();
    }

    private String toScheduleNewerMaxOffsetLockKey() {
        return SCHEDULE_NEWER_MAX_OFFSET_LOCK + SPLITER + consumerProxy.getTopic();
    }

    private String toNewerMaxOffsetLockTimeKey() {
        return NEWER_MAX_OFFSET_LOCK_TIME + SPLITER + consumerProxy.getTopic();
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

    /**
     * 更新较新的最大偏移量
     */
    public void updateNewerMaxOffset(List<ConsumerQueueOffset> consumerQueueOffsets, Collection<ConsumerProxy> consumerMap) {
        long start = System.currentTimeMillis();
        int newerMaxOffsetLockTime = getNewerMaxOffsetLockTime();
        // 锁定时间内只更新一次
        if (!lockNewerMaxOffset(newerMaxOffsetLockTime)) {
            return;
        }
        // 获取更新的topicStatsTable
        TopicStatsTable topicStatsTable = findUpdatedTopicStatsTable(consumerQueueOffsets);
        if (topicStatsTable == null) {
            log.debug("updateNewerMaxOffset no data:{} cost:{}ms", newerMaxOffsetLockTime, System.currentTimeMillis() - start);
            // 无数据，增加锁定时间
            incrNewerMaxOffsetLockTime(newerMaxOffsetLockTime);
            return;
        }
        // 多个消费者可能订阅同一个topic，那么每个消费者都需要更新最新的最大偏移量
        consumerMap.stream()
                .filter(proxy -> proxy.getTopic().equals(consumerProxy.getTopic()))
                .forEach(proxy -> proxy.updateNewerMaxOffset(topicStatsTable));
        // 重置锁定时间
        resetNewerMaxOffsetLockTime();
        long cost = System.currentTimeMillis() - start;
        if (cost > 800) {
            log.warn("updateNewerMaxOffset cost:{}ms", cost);
        } else {
            log.debug("updateNewerMaxOffset cost:{}ms", cost);
        }
    }

    /**
     * 查找更新的topicStatsTable
     */
    public TopicStatsTable findUpdatedTopicStatsTable(List<ConsumerQueueOffset> consumerQueueOffsets) {
        // 获取broker
        Set<String> brokers = consumerProxy.consumer().findBrokerAddressInSubscribe();
        if (brokers == null || brokers.isEmpty()) {
            return null;
        }
        // 转化为map
        Map<MessageQueue, ConsumerQueueOffset> allMQMap = consumerQueueOffsets.stream()
                .collect(Collectors.toMap(ConsumerQueueOffset::getMessageQueue, o -> o));
        // 随机打乱broker
        List<String> brokerList = new ArrayList<>(brokers);
        Collections.shuffle(brokerList);
        // 获取topicStatsTable
        for (String broker : brokerList) {
            TopicStatsTable updatedOffset = consumerProxy.consumer().getTopicStatsTable(broker);
            if (updatedOffset == null || updatedOffset.getOffsetTable().isEmpty()) {
                continue;
            }
            // 遍历topicStatsTable，判断是否有更新
            for (Entry<MessageQueue, TopicOffset> entry : updatedOffset.getOffsetTable().entrySet()) {
                ConsumerQueueOffset prevOffset = allMQMap.get(entry.getKey());
                if (prevOffset == null || entry.getValue().getMaxOffset() > prevOffset.getMaxOffset()) {
                    return updatedOffset;
                }
            }
        }
        return null;
    }

    /**
     * 更新较新的最大偏移量
     */
    public void updateNewerMaxOffset(Collection<ConsumerProxy> consumerProxies) {
        // 锁定时间内只更新一次
        if (!lockScheduleNewerMaxOffset()) {
            return;
        }
        // 获取更新的topicStatsTable
        List<TopicStatsTable> list = findTopicStatsTable();
        if (list == null || list.isEmpty()) {
            log.warn("updateNewerMaxOffset:{} no stats data", consumerProxy.getTopic());
            return;
        }
        log.info("updateNewerMaxOffset:{}", consumerProxy.getTopic());
        // 多个消费者可能订阅同一个topic，那么每个消费者都需要更新最新的最大偏移量
        for (ConsumerProxy consumerProxy : consumerProxies) {
            for (TopicStatsTable topicStatsTable : list) {
                consumerProxy.updateNewerMaxOffset(topicStatsTable);
            }
        }
    }

    /**
     * 查找topicStatsTable
     */
    public List<TopicStatsTable> findTopicStatsTable() {
        // 获取broker
        Set<String> brokers = consumerProxy.consumer().findBrokerAddressInSubscribe();
        if (brokers == null || brokers.isEmpty()) {
            return null;
        }
        List<TopicStatsTable> list = new ArrayList<>();
        // 获取topicStatsTable
        for (String broker : brokers) {
            TopicStatsTable updatedOffset = consumerProxy.consumer().getTopicStatsTable(broker);
            if (updatedOffset == null || updatedOffset.getOffsetTable().isEmpty()) {
                continue;
            }
            list.add(updatedOffset);
        }
        return list;
    }

    /**
     * 更新最大偏移量
     */
    public void updateNewerMaxOffset(TopicStatsTable topicStatsTable) {
        topicStatsTable.getOffsetTable().forEach((mq, offset) -> {
            try {
                if (!consumerProxy.isBroadcasting()) {
                    updateNewerMaxOffset(null, mq, offset.getMaxOffset());
                } else {
                    Set<String> clientIdSet = consumerProxy.getClientIdMap().keySet();
                    for (String cid : clientIdSet) {
                        updateNewerMaxOffset(cid, mq, offset.getMaxOffset());
                    }
                }
            } catch (Exception e) {
                log.error("updateNewerMaxOffset error", e);
            }
        });
    }

    /**
     * 更新较新最大偏移量
     */
    public void updateNewerMaxOffset(String clientId, MessageQueue messageQueue, long maxOffset) {
        redis.hset(toKey(clientId), toNewerMaxOffsetField(messageQueue), String.valueOf(maxOffset));
    }

    /**
     * 锁定定时较新的最大偏移量
     */
    private boolean lockScheduleNewerMaxOffset() {
        String rst = redis.set(toScheduleNewerMaxOffsetLockKey(), "", SetParams.setParams().nx().ex(MAX_SCHEDULE_NEWER_MAX_OFFSET_LOCK_EXPIRE));
        return "OK".equals(rst);
    }

    /**
     * 锁定较新的最大偏移量
     */
    private boolean lockNewerMaxOffset(int newerMaxOffsetLockTime) {
        String rst = redis.set(toNewerMaxOffsetLockKey(), "", SetParams.setParams().nx().ex(newerMaxOffsetLockTime));
        return "OK".equals(rst);
    }

    /**
     * 获取较新的最大偏移量锁过期时间
     */
    private int getNewerMaxOffsetLockTime() {
        String rst = redis.get(toNewerMaxOffsetLockTimeKey());
        return toLockTime(NumberUtils.toInt(rst, MIN_NEWER_MAX_OFFSET_LOCK_EXPIRE));
    }

    private void resetNewerMaxOffsetLockTime() {
        incrNewerMaxOffsetLockTime(MIN_NEWER_MAX_OFFSET_LOCK_EXPIRE - 1);
    }

    private void incrNewerMaxOffsetLockTime(int newerMaxOffsetLockTime) {
        if (newerMaxOffsetLockTime >= MAX_NEWER_MAX_OFFSET_LOCK_EXPIRE) {
            newerMaxOffsetLockTime = MIN_NEWER_MAX_OFFSET_LOCK_EXPIRE;
        }
        redis.set(toNewerMaxOffsetLockTimeKey(), String.valueOf(newerMaxOffsetLockTime + 1),
                SetParams.setParams().ex(newerMaxOffsetLockTime + 3));
    }

    private int toLockTime(int no) {
        if (no < MIN_NEWER_MAX_OFFSET_LOCK_EXPIRE) {
            return MIN_NEWER_MAX_OFFSET_LOCK_EXPIRE;
        }
        if (no > MAX_NEWER_MAX_OFFSET_LOCK_EXPIRE) {
            return MAX_NEWER_MAX_OFFSET_LOCK_EXPIRE;
        }
        return no;
    }
}
