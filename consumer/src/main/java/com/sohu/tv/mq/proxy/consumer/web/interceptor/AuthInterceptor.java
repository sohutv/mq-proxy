package com.sohu.tv.mq.proxy.consumer.web.interceptor;

import com.sohu.tv.mq.proxy.consumer.util.CipherHelper;
import com.sohu.tv.mq.proxy.model.MQException;
import com.sohu.tv.mq.proxy.util.WebUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 权限验证
 *
 * @author yongfeigao
 * @Description:
 * @date 2018年6月12日
 */
@Component
public class AuthInterceptor extends HandlerInterceptorAdapter {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private CipherHelper cipherHelper;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        String userString = WebUtil.getMQCloudUserFromHeader(request);
        if (StringUtils.isEmpty(userString)) {
            throw new MQException("no permission!");
        }
        String user = cipherHelper.decrypt(userString);
        if (user == null) {
            throw new MQException("access denied!");
        }
        WebUtil.setUserToAttribute(request, user);
        return true;
    }
}
