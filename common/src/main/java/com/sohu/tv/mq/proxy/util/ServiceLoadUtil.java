package com.sohu.tv.mq.proxy.util;

import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * 服务加载工具
 *
 * @author: yongfeigao
 * @date: 2022/7/22 11:39
 */
@Slf4j
public class ServiceLoadUtil {

    /**
     * 优先实例化interfaceClass，若interfaceClass不存在，则实例化defaultImplClass
     *
     * @param interfaceClass
     * @param defaultImplClass
     * @param <T>
     * @return
     */
    public static <T> T loadService(Class<T> interfaceClass, Class defaultImplClass) {
        ServiceLoader<T> loadedClass = ServiceLoader.load(interfaceClass);
        Iterator<T> iterator = loadedClass.iterator();
        if (iterator.hasNext()) {
            T t = iterator.next();
            log.info("ServiceLoader load service:{}", t);
            return t;
        }
        try {
            log.info("ServiceLoader load default service:{}", defaultImplClass);
            return (T) defaultImplClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
