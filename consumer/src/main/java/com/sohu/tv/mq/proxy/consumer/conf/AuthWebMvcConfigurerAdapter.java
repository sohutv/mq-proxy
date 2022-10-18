package com.sohu.tv.mq.proxy.consumer.conf;

import com.sohu.tv.mq.proxy.consumer.web.interceptor.AuthInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 拦截器配置
 *
 * @author yongfeigao
 * @Description:
 * @date 2018年6月12日
 */
@Configuration
public class AuthWebMvcConfigurerAdapter implements WebMvcConfigurer {

    @Autowired
    private AuthInterceptor authInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(authInterceptor).addPathPatterns("/mq/consumer/config", "/mq/consumer/unregister");
    }
}
