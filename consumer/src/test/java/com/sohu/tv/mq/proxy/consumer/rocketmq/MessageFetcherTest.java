package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.tv.mq.proxy.consumer.Application;
import com.sohu.tv.mq.proxy.consumer.config.ConsumerConfigManager;
import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.FetchResult;
import com.sohu.tv.mq.proxy.consumer.model.Message;
import com.sohu.tv.mq.proxy.consumer.rocketmq.ConsumerManager.ConsumerProxy;
import com.sohu.tv.mq.proxy.consumer.web.param.ConsumeParam;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

/**
 * @author: yongfeigao
 * @date: 2022/5/30 10:25
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class MessageFetcherTest {

    String topic = "mqcloud-http-test-topic";
    String consumer = "clustering-mqcloud-http-consumer";
    String consumerBroadcast = "broadcast-mqcloud-http-consumer";

    @Autowired
    MessageFetcher messageFetcher;

    @Autowired
    private ConsumerManager consumerManager;

    @Autowired
    private ConsumerConfigManager consumerConfigManager;

    @Test
    public void testFetchClustering() throws Exception {
        ConsumeParam consumeParam = new ConsumeParam();
        consumeParam.setTopic(topic);
        consumeParam.setConsumer(consumer);
        int counter = 0;
        while (true) {
            MQProxyResponse<FetchResult> response = messageFetcher.fetch(FetchRequest.build(consumeParam));
            if (response.ok()) {
                FetchResult fetchResult = response.getResult();
                if (fetchResult != null && fetchResult.getMsgListSize() > 0) {
                    System.out.println(fetchResult.getStatus() + "==" + fetchResult.getMsgListSize());
                    counter += fetchResult.getMsgListSize();
                }
                consumeParam.setRequestId(response.getResult().getRequestId());
            } else {
                System.out.println("--" + response.getMessage());
            }
            if (Math.random() > 0.9) {
                System.out.println(counter);
            }
        }
    }

    @Test
    public void testFetchBroadcast() throws Exception {
        ConsumeParam consumeParam = new ConsumeParam();
        consumeParam.setTopic(topic);
        consumeParam.setConsumer(consumerBroadcast);
        consumeParam.setClientId("a");
        int counter = 0;
        while (true) {
            MQProxyResponse<FetchResult> response = messageFetcher.fetch(FetchRequest.build(consumeParam));
            if (response.ok()) {
                FetchResult fetchResult = response.getResult();
                if (fetchResult != null && fetchResult.getMsgListSize() > 0) {
                    System.out.println(fetchResult.getStatus() + "==" + fetchResult.getMsgListSize());
                    counter += fetchResult.getMsgListSize();
                }
                consumeParam.setRequestId(response.getResult().getRequestId());
            } else {
                System.out.println("--" + response.getMessage());
            }
            if (Math.random() > 0.9) {
                System.out.println(counter);
            }
        }
    }

    @Test
    public void testFetchRetryMessages() {
        String msgId = "7F0000010008323659F8269857B513A9";
        ConsumerProxy consumerProxy = consumerManager.getConsumer(consumer);
        consumerConfigManager.updateRetryMsgId(consumerProxy, msgId);
        List<Message> messages = messageFetcher.fetchRetryMessages(consumerProxy, null);
        Assert.assertEquals(msgId, messages.get(0).getMessageId());
    }
}