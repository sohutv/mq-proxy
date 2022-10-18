package com.sohu.tv.mq.proxy.test.rocketmq;

import com.sohu.tv.mq.proxy.store.IRedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

/**
 * @author: yongfeigao
 * @date: 2022/7/11 18:12
 */
public abstract class HttpMQ {
    protected Logger log = LoggerFactory.getLogger(getClass());
    protected String group;
    protected String topic;
    protected boolean running = true;

    protected int intervalInMillis = 1;

    protected RestTemplate restTemplate;
    protected IRedis redis;

    public HttpMQ(RestTemplate restTemplate, String group, String topic) {
        this.restTemplate = restTemplate;
        this.group = group;
        this.topic = topic;
    }

    public void start() {
        Thread thread = new Thread() {
            public void run() {
                while (running) {
                    try {
                        long count = HttpMQ.this.run();
                        record(count);
                        if (getIntervalInMillis() > 0) {
                            Thread.sleep(getIntervalInMillis());
                        }
                    } catch (Throwable e) {
                        log.error("topic:{} group:{}", getTopic(), getGroup(), e);
                    }
                }
                HttpMQ.this.stop();
            }
        };
        thread.start();
    }

    protected abstract long run();

    protected void stop() {

    }

    public int getIntervalInMillis() {
        return intervalInMillis;
    }

    public void setIntervalInMillis(int intervalInMillis) {
        this.intervalInMillis = intervalInMillis;
    }

    protected void record(long count) {
        if (count > 0) {
            redis.incrBy(buildRecordKey(), count);
        }
    }

    protected String buildRecordKey() {
        return topic + ":" + group;
    }

    public String getCount() {
        return redis.get(buildRecordKey());
    }

    public String getGroup() {
        return group;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void setRedis(IRedis redis) {
        this.redis = redis;
    }

    public void shutdown() {
        setRunning(false);
        redis.del(buildRecordKey());
    }
}
