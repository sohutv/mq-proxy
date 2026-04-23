package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.FetchResult;
import com.sohu.tv.mq.proxy.consumer.model.Message;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.util.CollectionUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * 消息拉取器
 */
public interface MessageFetcher {

    /**
     * 拉取消息
     */
    MQProxyResponse<FetchResult> fetch(FetchRequest request) throws Exception;

    /**
     * 提交ack
     */
    MQProxyResponse<String> commitAck(FetchRequest request) throws Exception;

    /**
     * 反序列化消息
     */
    default List<Message> deserializeMessages(ConsumerProxy consumerProxy, List<MessageExt> msgList) throws Exception {
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
     */
    default Message deserializeMessage(ConsumerProxy consumerProxy, MessageExt msg) throws Exception {
        Message message = new Message();
        message.setMessage(consumerProxy.deserialize(msg));
        message.setMessageId(msg.getMsgId());
        message.setTimestamp(msg.getStoreTimestamp());
        return message;
    }
}
