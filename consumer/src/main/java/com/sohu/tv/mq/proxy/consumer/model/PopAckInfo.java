package com.sohu.tv.mq.proxy.consumer.model;

import lombok.Data;

@Data
public class PopAckInfo {
    private String brokerName;
    private int queueId;
    private long offset;
    private long nextOffset;
    private long lockTime;
    private boolean forceAck;
    private long expireTime;
    private String requestId;

    private int retryTimes;
    private String clientIp;

    public static PopAckInfo build(String requestId, long expireTime, int retryTimes, String clientIp) {
        FetchRequest req = FetchRequest.build(requestId);
        PopAckInfo popAckInfo = new PopAckInfo();
        popAckInfo.setBrokerName(req.getBrokerName());
        popAckInfo.setQueueId(req.getQueueId());
        popAckInfo.setOffset(req.getOffset());
        popAckInfo.setNextOffset(req.getNextOffset());
        popAckInfo.setLockTime(req.getLockTime());
        popAckInfo.setForceAck(req.isForceAck());
        popAckInfo.setExpireTime(expireTime);
        popAckInfo.setRequestId(requestId);
        popAckInfo.setRetryTimes(retryTimes);
        popAckInfo.setClientIp(clientIp);
        return popAckInfo;
    }
}
