package com.sohu.tv.mq.proxy.consumer.model;

import com.sohu.tv.mq.proxy.consumer.web.param.ConsumeParam;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;

/**
 * 抓取请求
 *
 * @author: yongfeigao
 * @date: 2022/6/9 14:25
 */
@Data
public class FetchRequest {
    private String topic;
    private String consumer;
    private String brokerName;
    private int queueId;
    // 当前偏移量
    private long offset;
    // 下一次消费的偏移量
    private long nextOffset;
    // 广播消费时的不同的客户端标识，不同的clientId消费全量消息
    private String clientId;
    // 队列锁定时间戳
    private long lockTime;
    // 请求id
    private String requestId;
    /**
     * 构建实例
     * @param param
     * @return
     */
    public static FetchRequest build(ConsumeParam param) {
        FetchRequest fetchRequest = new FetchRequest();
        fetchRequest.setTopic(param.getTopic());
        fetchRequest.setConsumer(param.getConsumer());
        fetchRequest.setRequestId(param.getRequestId());
        if (StringUtils.isNotBlank(param.getClientId())) {
            fetchRequest.setClientId(param.getClientId());
        }
        fetchRequest.decode();
        return fetchRequest;
    }

    /**
     * 重置属性
     * @param consumerQueueOffset
     * @return
     */
    public FetchRequest reset(ConsumerQueueOffset consumerQueueOffset, long nextOffset){
        setBrokerName(consumerQueueOffset.getMessageQueue().getBrokerName());
        setQueueId(consumerQueueOffset.getMessageQueue().getQueueId());
        setOffset(consumerQueueOffset.getCommittedOffset());
        setLockTime(consumerQueueOffset.getLockTimestamp());
        setNextOffset(nextOffset);
        return this;
    }

    /**
     * 编码
     * @return
     */
    public String encode() {
        StringBuilder builder = new StringBuilder();
        builder.append(brokerName).append(",")
                .append(queueId).append(",")
                .append(offset).append(",")
                .append(nextOffset).append(",")
                .append(lockTime);
        return Base64.getUrlEncoder().encodeToString(builder.toString().getBytes());
    }

    /**
     * 解码
     * @param str
     */
    public void decode() {
        if (StringUtils.isBlank(requestId)) {
            return;
        }
        String decodeStr = new String(Base64.getUrlDecoder().decode(requestId));
        String[] strArray = decodeStr.split(",");
        brokerName = strArray[0];
        queueId = Integer.parseInt(strArray[1]);
        offset = Long.parseLong(strArray[2]);
        nextOffset = Long.parseLong(strArray[3]);
        lockTime = Long.parseLong(strArray[4]);
    }

    /**
     * offset是否非法
     *
     * @return
     */
    public boolean isOffsetIllegal() {
        return brokerName == null || offset < 0 || nextOffset < offset;
    }
}
