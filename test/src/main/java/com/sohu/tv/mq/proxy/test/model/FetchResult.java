package com.sohu.tv.mq.proxy.test.model;

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
    // 请求id，每次请求均需要携带
    private String requestId;
    // 消息列表
    private List<Message> msgList;
    // 重试消息列表
    private List<Message> retryMsgList;

    public int getMsgListSize() {
        return msgList == null ? 0 : msgList.size();
    }

    public int getRetryMsgListSize() {
        return retryMsgList == null ? 0 : retryMsgList.size();
    }
}
