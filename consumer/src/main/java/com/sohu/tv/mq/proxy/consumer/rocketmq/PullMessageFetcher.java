package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.index.tv.mq.common.PullResponse;
import com.sohu.tv.mq.proxy.consumer.config.ConsumerConfigManager;
import com.sohu.tv.mq.proxy.consumer.model.*;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.consumer.web.param.ConsumeParam;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * pull方式消息抓取器
 *
 * @author: yongfeigao
 * @date: 2022/5/27 15:48
 */
@Slf4j
@Component
public class PullMessageFetcher implements MessageFetcher {

    @Autowired
    private ConsumerManager consumerManager;

    @Autowired
    private ConsumerConfigManager consumerConfigManager;

    /**
     * 拉取消息
     */
    public MQProxyResponse<FetchResult> fetch(FetchRequest request) throws Exception {
        ConsumerProxy consumer = getConsumerProxy(request);
        return fetch(request, consumer);
    }

    @Override
    public MQProxyResponse<String> commitAck(FetchRequest request) throws Exception {
        ConsumerProxy consumer = getConsumerProxy(request);
        return commitAck(request, consumer);
    }

    /**
     * 拉取消息
     */
    public MQProxyResponse<FetchResult> fetch(FetchRequest request, ConsumerProxy consumer) throws Exception {
        // 先提交上次消费的消息的ack
        MQProxyResponse<String> ackResponse = commitAck(request, consumer);
        // 拉取消息
        MQProxyResponse<FetchResult> fetchResultResponse = fetchMessage(request, consumer);
        // 拉取结果处理
        return afterFetch(request, consumer, ackResponse, fetchResultResponse);
    }

    /**
     * 提交ack
     */
    public MQProxyResponse<String> commitAck(FetchRequest request, ConsumerProxy consumer) {
        return consumer.commitAck(request);
    }

    /**
     * 拉取消息
     */
    public MQProxyResponse<FetchResult> fetchMessage(FetchRequest request, ConsumerProxy consumer) throws Exception {
        // 选择合适的队列
        ConsumerQueueOffsetResult consumerQueueOffsetResult = consumer.choose(request.getClientId());
        if (consumerQueueOffsetResult == null || consumerQueueOffsetResult.getChosenConsumerQueueOffset() == null) {
            return MQProxyResponse.buildErrorResponse("no consumerQueueOffsetResult");
        }
        ConsumerQueueOffset queueOffset = consumerQueueOffsetResult.getChosenConsumerQueueOffset();
        // 拉取结果
        FetchResult fetchResult = null;
        // 是否需要解锁
        boolean unlock = false;
        // 是否已经解锁
        boolean unlocked = false;
        try {
            // 消息拉取
            PullResponse pullResponse = consumer.pull(queueOffset.getMessageQueue(),
                    queueOffset.getCommittedOffset(), request.getClientIp());
            // 没有新消息，尝试更新最大offset
            if (pullResponse.isNoNewMsgStatus()) {
                consumerManager.updateNewerMaxOffset(consumerQueueOffsetResult.getConsumerQueueOffsets(), consumer);
            }
            // offset错误需要修正
            if (pullResponse.isOffsetErrorStatus()) {
                log.warn("pull from {}:{} queueOffset:{} status:{}", request.getTopic(), request.getConsumer(),
                        queueOffset, pullResponse.getStatus());
                queueOffset.setMaxOffset(pullResponse.getMaxOffset() - 1);
                request.setForceAck(true);
            }
            // 更新最大offset
            consumer.updateMaxOffset(request, queueOffset, pullResponse);
            // 已经提交过偏移量的消费者但是未拉取到消息需要解锁
            if (!queueOffset.isFirstConsume() && !pullResponse.isFoundStatus() && !pullResponse.isOffsetErrorStatus()) {
                unlock = true;
            }
            // 重置下次需要的数据
            request.reset(queueOffset, pullResponse.getNextOffset());
            // 构建结果
            fetchResult = buildFetchResult(consumer, pullResponse);
            fetchResult.setQueueInfo(queueOffset);
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
     * 拉取结果处理
     */
    public MQProxyResponse<FetchResult> afterFetch(FetchRequest request, ConsumerProxy consumer, MQProxyResponse<String> ackResponse, MQProxyResponse<FetchResult> fetchResultResponse) throws Exception {
        if (fetchResultResponse.ok()) {
            fetchResultResponse.getResult().setAckInfo(ackResponse);
        }
        return fetchResultResponse;
    }

    /**
     * 拉取重试消息
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
     */
    public FetchResult buildFetchResult(ConsumerProxy consumerProxy, PullResponse pullResponse) throws Exception {
        FetchResult fetchResult = new FetchResult();
        fetchResult.setStatus(pullResponse.getStatus());
        fetchResult.setMsgList(deserializeMessages(consumerProxy, pullResponse.getMsgList()));
        return fetchResult;
    }

    public ConsumerProxy getConsumerProxy(FetchRequest request) throws Exception {
        ConsumerProxy consumer = consumerManager.getConsumer(request);
        if (request.getMaxPullSize() != null && request.getMaxPullSize() > 0 && request.getMaxPullSize() < 128) {
            consumer.setMaxPullSize(request.getMaxPullSize());
        }
        if (request.getPullTimeout() != null && request.getPullTimeout() >= 1 && request.getPullTimeout() <= 60) {
            consumer.setConsumerPullTimeoutMillis(request.getPullTimeout() * 1000);
        }
        if (request.getConsumeTimeout() != null && request.getConsumeTimeout() >= 10 && request.getConsumeTimeout() <= 129600) {
            consumer.setConsumeTimeoutInMillis(request.getConsumeTimeout() * 1000);
        }
        consumer.setPop(request.isPop());
        return consumer;
    }
}
