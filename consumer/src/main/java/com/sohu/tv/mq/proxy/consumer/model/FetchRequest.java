package com.sohu.tv.mq.proxy.consumer.model;

import com.sohu.tv.mq.proxy.consumer.web.param.ConsumeParam;
import com.sohu.tv.mq.proxy.util.WebUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
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
    // 是否强制ack
    private boolean forceAck;
    // 客户端ip
    private String clientIp;

    /**
     * 构建实例
     */
    public static FetchRequest build(ConsumeParam param) {
        return build(null, param);
    }

    /**
     * 构建实例
     */
    public static FetchRequest build(HttpServletRequest request, ConsumeParam param) {
        FetchRequest fetchRequest = new FetchRequest();
        fetchRequest.setTopic(param.getTopic());
        fetchRequest.setConsumer(param.getConsumer());
        fetchRequest.setRequestId(param.getRequestId());
        if (StringUtils.isNotBlank(param.getClientId())) {
            fetchRequest.setClientId(param.getClientId());
        }
        if (param.isClientIpBlank() && request != null) {
            fetchRequest.setClientIp(WebUtil.getIp(request));
        } else {
            fetchRequest.setClientIp(param.getClientIp());
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
                .append(lockTime).append(",")
                .append(forceAck);
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
        if (strArray.length > 5) {
            forceAck = Boolean.parseBoolean(strArray[5]);
        }
    }

    /**
     * offset是否非法
     *
     * @return
     */
    public boolean isOffsetIllegal() {
        return brokerName == null || offset < 0 || (nextOffset < offset && !forceAck);
    }

    public String getAckInfo() {
        return brokerName + "-" + queueId + ":" + offset + "," + nextOffset + "," + lockTime + "," + forceAck;
    }
}
