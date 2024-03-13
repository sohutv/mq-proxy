package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.index.tv.mq.common.PullResponse;
import com.sohu.tv.mq.proxy.consumer.config.ConsumerConfigManager;
import com.sohu.tv.mq.proxy.consumer.model.ConsumerQueueOffset;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.FetchResult;
import com.sohu.tv.mq.proxy.consumer.model.Message;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.LinkedList;
import java.util.List;

import static com.sohu.index.tv.mq.common.PullResponse.Status.NO_MATCHED_MSG;
import static com.sohu.index.tv.mq.common.PullResponse.Status.OFFSET_ILLEGAL;

/**
 * 消息抓取器
 *
 * @author: yongfeigao
 * @date: 2022/5/27 15:48
 */
@Slf4j
@Component
public class MessageFetcher {

    @Autowired
    private ConsumerManager consumerManager;

    @Autowired
    private ConsumerConfigManager consumerConfigManager;

    /**
     * 拉取消息
     *
     * @param request
     * @return
     */
    public MQProxyResponse<FetchResult> fetch(FetchRequest request) throws Exception {
        // 获取消费代理
        ConsumerProxy consumer = consumerManager.getConsumer(request);
        // offset ack
        consumer.offsetAck(request);
        // 选择合适的队列
        ConsumerQueueOffset queueOffset = consumer.choose(request.getClientId());
        if (queueOffset == null) {
            return MQProxyResponse.buildErrorResponse("no queue choose");
        }
        // 拉取结果
        FetchResult fetchResult = null;
        // 是否需要解锁
        boolean unlock = false;
        // 是否已经解锁
        boolean unlocked = false;
        try {
            // 消息拉取
            PullResponse pullResponse = consumer.pull(queueOffset.getMessageQueue(), queueOffset.getCommittedOffset());
            if (OFFSET_ILLEGAL == pullResponse.getStatus() || NO_MATCHED_MSG == pullResponse.getStatus()) {
                log.warn("pull from {}:{} queueOffset:{} status:{}", request.getTopic(), request.getConsumer(),
                        queueOffset, pullResponse.getStatus());
                queueOffset.setMaxOffset(pullResponse.getMaxOffset() - 1);
                request.setForceAck(true);
            }
            // 更新最大offset
            consumer.updateMaxOffset(request.getClientId(), queueOffset, pullResponse.getMaxOffset());
            // 已经提交过偏移量的消费者但是未拉取到消息需要解锁
            if (!queueOffset.isFirstConsume() && !pullResponse.isStatusFound() &&
                    (OFFSET_ILLEGAL != pullResponse.getStatus() && NO_MATCHED_MSG != pullResponse.getStatus())) {
                unlock = true;
            }
            // 重置下次需要的数据
            request.reset(queueOffset, pullResponse.getNextOffset());
            // 构建结果
            fetchResult = buildFetchResult(consumer, pullResponse);
            // 拉取重试消息
            fetchResult.setRetryMsgList(fetchRetryMessages(consumer, request.getClientId()));
        } catch (Exception e) {
            unlock = true;
            log.error("pull from {}:{} queueOffset:{}", request.getTopic(), request.getConsumer(), queueOffset, e);
            return MQProxyResponse.buildErrorResponse(e.toString());
        } finally {
            // 未拉取到消息或异常需要解锁，以便其他消费者消费该队列
            if (unlock) {
                unlocked = consumer.unlock(request.getClientId(), queueOffset.getMessageQueue());
            }
        }
        // 未解锁需要设置requestId，以便下次消费时ack
        if (!unlocked) {
            fetchResult.setRequestId(request.encode());
        }
        return MQProxyResponse.buildOKResponse(fetchResult);
    }

    /**
     * 拉取重试消息
     *
     * @param consumer
     * @param clientId
     * @return
     */
    public List<Message> fetchRetryMessages(ConsumerProxy consumer, String clientId) {
        return consumerConfigManager.consumeRetryMsgIds(consumer.getGroup(), clientId, msgId -> {
            try {
                MessageExt messageExt = consumer.viewMessage(consumer.getTopic(), msgId);
                return deserializeMessage(consumer, messageExt);
            } catch (Exception e) {
                log.error("fetch topic:{} msg:{}", consumer.getTopic(), msgId, e);
                return null;
            }
        });
    }

    /**
     * 构建返回结果
     * @param consumerProxy
     * @param pullResponse
     * @return
     * @throws Exception
     */
    public FetchResult buildFetchResult(ConsumerProxy consumerProxy, PullResponse pullResponse) throws Exception {
        FetchResult fetchResult = new FetchResult();
        fetchResult.setStatus(pullResponse.getStatus());
        fetchResult.setMsgList(deserializeMessages(consumerProxy, pullResponse.getMsgList()));
        return fetchResult;
    }

    /**
     * 反序列化消息
     *
     * @param pullConsumer
     * @param pullResult
     * @return
     * @throws Exception
     */
    private List<Message> deserializeMessages(ConsumerProxy consumerProxy, List<MessageExt> msgList) throws Exception {
        if (CollectionUtils.isEmpty(msgList)) {
            return null;
        }
        List<Message> proxyMessages = new LinkedList<>();
        for (MessageExt msg : msgList) {
            proxyMessages.add(deserializeMessage(consumerProxy, msg));
        }
        return proxyMessages;
    }

    /**
     * 反序列化消息
     *
     * @param consumerProxy
     * @param msg
     * @return
     * @throws Exception
     */
    private Message deserializeMessage(ConsumerProxy consumerProxy, MessageExt msg) throws Exception {
        Message message = new Message();
        message.setMessage(consumerProxy.deserialize(msg));
        message.setMessageId(msg.getMsgId());
        message.setTimestamp(msg.getStoreTimestamp());
        return message;
    }
}
