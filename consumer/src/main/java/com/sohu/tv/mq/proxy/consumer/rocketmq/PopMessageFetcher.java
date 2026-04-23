package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.index.tv.mq.common.PullResponse;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.FetchResult;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageQueue;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

/**
 * pop方式消息拉取器
 */
@Slf4j
@Component
public class PopMessageFetcher extends PullMessageFetcher {

    public MQProxyResponse<String> commitAck(FetchRequest request, ConsumerProxy consumer) {
        return consumer.commitPopAck(request);
    }

    public MQProxyResponse<FetchResult> fetchMessage(FetchRequest request, ConsumerProxy consumer) throws Exception {
        // 优先拉取重试消息
        FetchResult fetchResult = fetchPopRetryMessages(consumer, request);
        if (fetchResult != null) {
            return MQProxyResponse.buildOKResponse(fetchResult);
        }
        return super.fetchMessage(request, consumer);
    }

    @Override
    public MQProxyResponse<FetchResult> afterFetch(FetchRequest request, ConsumerProxy consumer, MQProxyResponse<String> ackResponse, MQProxyResponse<FetchResult> fetchResultResponse) throws Exception {
        if (!fetchResultResponse.ok() || fetchResultResponse.getResult().getRequestId() == null ||
                fetchResultResponse.getResult().isPopRetryMsg()) {
            return super.afterFetch(request, consumer, ackResponse, fetchResultResponse);
        }
        FetchResult fetchResult = fetchResultResponse.getResult();
        // pop记录处理中的ack
        FetchRequest req = FetchRequest.build(fetchResult.getRequestId());
        req.setClientId(request.getClientId());
        req.setClientIp(request.getClientIp());
        consumer.saveProcessingPopAck(req);
        // pop模式不需要锁定队列，直接ack
        consumer.commitAck(request);
        return super.afterFetch(request, consumer, ackResponse, fetchResultResponse);
    }

    /**
     * 拉取pop重试消息
     */
    public FetchResult fetchPopRetryMessages(ConsumerProxy consumer, FetchRequest request) {
        FetchRequest popAckInfo = consumer.getTimeoutPopAckInfo(request);
        if (popAckInfo == null) {
            return null;
        }
        try {
            log.warn("pop retry req:{}", popAckInfo);
            MessageQueue mq = new MessageQueue(consumer.getTopic(), popAckInfo.getBrokerName(), popAckInfo.getQueueId());
            PullResponse pullResponse = consumer.pull(mq, popAckInfo.getOffset(), popAckInfo.getPullSize(), request.getClientIp());
            if (CollectionUtils.isEmpty(pullResponse.getMsgList())) {
                log.error("pop retry topic:{} req:{} pullResponse:{}", consumer.getTopic(), popAckInfo, pullResponse);
                // 消息不存在需要ack掉，以免一直重试
                consumer.commitPopAck(popAckInfo);
                return null;
            }
            FetchResult fetchResult = buildRetryFetchResult(consumer, pullResponse);
            fetchResult.setRequestId(popAckInfo.getRequestId());
            return fetchResult;
        } catch (Exception e) {
            log.error("pop retry topic:{} req:{}", consumer.getTopic(), request, e);
        }
        return null;
    }

    public FetchResult buildRetryFetchResult(ConsumerProxy consumerProxy, PullResponse pullResponse) throws Exception {
        FetchResult fetchResult = new FetchResult();
        fetchResult.setStatus(pullResponse.getStatus());
        fetchResult.setRetryMsgList(deserializeMessages(consumerProxy, pullResponse.getMsgList()));
        fetchResult.setPopRetryMsg(true);
        return fetchResult;
    }
}
