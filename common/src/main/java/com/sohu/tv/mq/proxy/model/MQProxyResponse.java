package com.sohu.tv.mq.proxy.model;

import lombok.Data;

/**
 * MQProxy响应
 *
 * @author: yongfeigao
 * @date: 2022/5/31 10:36
 */
@Data
public class MQProxyResponse<T> {
    // 请求状态
    private int status = 200;
    // 提示信息
    private String message;
    // 响应体
    private T result;

    public boolean ok() {
        return 200 == status;
    }

    public static <T> MQProxyResponse<T> buildOKResponse() {
        return new MQProxyResponse();
    }

    public static <T> MQProxyResponse<T> buildOKResponse(T result) {
        if (result != null) {
            return buildOKResponse().resetResult(result);
        }
        return buildOKResponse().resetStatus(201);
    }

    public static <T> MQProxyResponse<T> buildOKResponse(String message) {
        MQProxyResponse mqProxyResponse = new MQProxyResponse();
        mqProxyResponse.setMessage(message);
        return mqProxyResponse;
    }

    public static <T> MQProxyResponse<T> buildUnknownResponse(String message) {
        return buildOKResponse(message).resetStatus(300);
    }

    public static <T> MQProxyResponse<T> buildParamErrorResponse(String message) {
        return buildOKResponse(message).resetStatus(400);
    }

    public static <T> MQProxyResponse<T> buildErrorResponse(String message) {
        return buildOKResponse(message).resetStatus(500);
    }

    public <T> MQProxyResponse<T> resetStatus(int status) {
        this.status = status;
        return (MQProxyResponse<T>) this;
    }

    public <K> MQProxyResponse<K> resetResult(K result) {
        this.result = (T) result;
        return (MQProxyResponse<K>) this;
    }
}
