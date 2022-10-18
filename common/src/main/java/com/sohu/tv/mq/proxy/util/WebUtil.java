package com.sohu.tv.mq.proxy.util;

import org.apache.commons.lang3.StringUtils;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * web相关工具
 * @Description: 
 * @author yongfeigao
 * @date 2018年6月12日
 */
public class WebUtil {

    public static final String MQCLOUD_USER_TOKEN = "MU";

    /**
     * 从request中获取客户端ip
     * 
     * @param request
     * @return
     */
    public static String getIp(ServletRequest request) {
        HttpServletRequest req = (HttpServletRequest) request;
        String addr = getHeaderValue(req, "X-Forwarded-For");
        if (StringUtils.isNotEmpty(addr) && addr.contains(",")) {
            addr = addr.split(",")[0];
        }
        if (StringUtils.isEmpty(addr)) {
            addr = getHeaderValue(req, "X-Real-IP");
        }
        if (StringUtils.isEmpty(addr)) {
            addr = req.getRemoteAddr();
        }
        return addr;
    }
    
    /**
     * 获取请求的完整url
     * @param request
     * @return
     */
    public static String getUrl(HttpServletRequest request) {
        String url = request.getRequestURL().toString();
        String queryString = request.getQueryString();
        if(queryString != null) {
            url += "?" + request.getQueryString();
        }
        return url;
    }
    
    /**
     * 获取ServletRequest header value
     * @param request
     * @param name
     * @return
     */
    public static String getHeaderValue(HttpServletRequest request, String name) {
        String v = request.getHeader(name);
        if(v == null) {
            return null;
        }
        return v.trim();
    }

    public static String getMQCloudUserFromHeader(HttpServletRequest request){
        return getHeaderValue(request, MQCLOUD_USER_TOKEN);
    }

    public static void setUserToAttribute(ServletRequest request, String user) {
        request.setAttribute(MQCLOUD_USER_TOKEN, user);
    }

    public static String getUserFromAttribute(ServletRequest request) {
        return (String) request.getAttribute(MQCLOUD_USER_TOKEN);
    }

    /**
     * 从request属性中获取对象
     * @param request
     * @return
     */
    public static void setAttribute(ServletRequest request, String name, Object obj) {
        request.setAttribute(name, obj);
    }
    
    /**
     * 设置对象到request属性中
     * @param request
     * @return
     */
    public static Object getAttribute(ServletRequest request, String name) {
        return request.getAttribute(name);
    }
    
    /**
     * 跳转
     * @param response
     * @param request
     * @param path
     * @throws IOException 
     */
    public static void redirect(HttpServletResponse response, HttpServletRequest request, String path) throws IOException {
        response.sendRedirect(request.getContextPath() + path);
    }
}
