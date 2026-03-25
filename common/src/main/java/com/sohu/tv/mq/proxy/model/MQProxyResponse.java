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
    public static final int OK_STATUS = 200;
    public static final int ERROR_STATUS = 500;
    public static final int PARAM_ERROR_STATUS = 400;
    // 请求状态
    private int status = OK_STATUS;
    // 提示信息
    private String message;
    // 响应体
    private T result;

    public boolean ok() {
        return OK_STATUS == status;
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
        return buildOKResponse(message).resetStatus(PARAM_ERROR_STATUS);
    }

    public static <T> MQProxyResponse<T> buildParamErrorResponse(String message, T result) {
        return buildParamErrorResponse(message).resetResult(result);
    }

    public static <T> MQProxyResponse<T> buildErrorResponse(String message) {
        return buildOKResponse(message).resetStatus(ERROR_STATUS);
    }

    public static <T> MQProxyResponse<T> buildErrorResponse(String message, T result) {
        return buildErrorResponse(message).resetResult(result);
    }

    public <T> MQProxyResponse<T> resetStatus(int status) {
        this.status = status;
        return (MQProxyResponse<T>) this;
    }

    public <K> MQProxyResponse<K> resetResult(K result) {
        this.result = (T) result;
        return (MQProxyResponse<K>) this;
    }

    public <K> MQProxyResponse<K> resetMessage(String message) {
        this.message = message;
        return (MQProxyResponse<K>) this;
    }

    public T removeResult() {
        T result = this.result;
        this.result = null;
        return result;
    }
}
