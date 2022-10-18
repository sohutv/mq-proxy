package com.sohu.tv.mq.proxy.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 配置中心
 *
 * @author: yongfeigao
 * @date: 2022/6/24 11:37
 */
public abstract class MQConfigCenter<T> {

    protected Logger log = LoggerFactory.getLogger(getClass());

    // 资源
    private Set<T> configs;

    // 定时任务执行
    private ScheduledExecutorService taskExecutorService;

    public MQConfigCenter() {
        taskExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, getClass().getName() + "Thread");
            }
        });
        start();
    }

    /**
     * 启动更新任务
     */
    public void start() {
        // 启动消费者配置同步任务
        taskExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    updateConfig();
                } catch (Throwable e) {
                    log.error("updateConfig error", e);
                }
            }
        }, initialDelayInMillis(), periodInMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 更新配置
     */
    public void updateConfig() {
        Set<T> oldConfigs = configs;
        // 获取新的配置
        this.configs = fetchConfigs();
        boolean configChanged = false;
        // 新的配置为null，需要清空之前的配置
        if (configs == null) {
            if (oldConfigs != null) {
                configChanged = true;
                for (T config : oldConfigs) {
                    log.info("delete config:{}", config);
                    deleteConfig(config);
                }
            }
        } else {
            // 1.添加新的
            for (T config : configs) {
                if (oldConfigs == null || !oldConfigs.contains(config)) {
                    configChanged = true;
                    log.info("add config:{}", config);
                    addConfig(config);
                }
            }
            // 2.删除不存在的
            if (oldConfigs != null) {
                for (T config : oldConfigs) {
                    if (!configs.contains(config)) {
                        configChanged = true;
                        log.info("delete config:{}", config);
                        deleteConfig(config);
                    }
                }
            }
        }
        if (configChanged) {
            log.info("config changed from {} to {}", oldConfigs, configs);
        }
    }

    /**
     * 删除配置
     *
     * @param config
     */
    public abstract void deleteConfig(T config);

    /**
     * 添加配置
     *
     * @param config
     */
    public abstract void addConfig(T config);

    /**
     * 初始任务执行延时
     *
     * @return
     */
    protected long initialDelayInMillis() {
        return 30 * 1000;
    }

    /**
     * 周期执行间隔
     *
     * @return
     */
    protected long periodInMillis() {
        return 60 * 1000;
    }

    /**
     * 抓取配置
     *
     * @return
     */
    protected abstract Set<T> fetchConfigs();

    public void shutdown() {
        taskExecutorService.shutdown();
    }
}
