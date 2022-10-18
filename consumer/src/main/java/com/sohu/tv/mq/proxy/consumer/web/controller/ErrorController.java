package com.sohu.tv.mq.proxy.consumer.web.controller;

import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import com.sohu.tv.mq.proxy.util.WebUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * 错误统一处理
 *
 * @author: yongfeigao
 * @date: 2022/5/31 11:15
 */
@Slf4j
@ControllerAdvice
public class ErrorController {
    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public MQProxyResponse<?> defaultErrorHandler(HttpServletRequest req, Exception e) throws Exception {
        log.error("ip:{} url:{}", WebUtil.getIp(req), WebUtil.getUrl(req), e);
        if (e instanceof BindException) {
            return MQProxyResponse.buildParamErrorResponse(e.toString());
        }
        return MQProxyResponse.buildErrorResponse(e.toString());
    }
}
