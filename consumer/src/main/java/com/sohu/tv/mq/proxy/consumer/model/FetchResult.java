package com.sohu.tv.mq.proxy.consumer.model;

import com.sohu.index.tv.mq.common.PullResponse.Status;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import lombok.Data;

import java.util.List;

/**
 * 抓取结果
 *
 * @author: yongfeigao
 * @date: 2022/6/6 11:02
 */
@Data
public class FetchResult {
    // 拉取状态
    private Status status;
    // 请求id，每次请求均需要携带
    private String requestId;
    // 消息列表
    private List<Message> msgList;
    // 重试消息列表
    private List<Message> retryMsgList;
    // 队列信息
    private ConsumerQueueOffset queueInfo;
    // ack响应
    private MQProxyResponse ackInfo;

    public int getMsgListSize() {
        return msgList == null ? 0 : msgList.size();
    }

    public int getRetryMsgListSize() {
        return retryMsgList == null ? 0 : retryMsgList.size();
    }
}
