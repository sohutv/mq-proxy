package com.sohu.tv.mq.proxy.consumer.rocketmq;

import com.sohu.tv.mq.proxy.consumer.model.FetchRequest;
import com.sohu.tv.mq.proxy.consumer.model.FetchResult;
import com.sohu.tv.mq.proxy.model.MQProxyResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DefaultMessageFetcher implements MessageFetcher {

    @Autowired
    private PullMessageFetcher pullMessageFetcher;

    @Autowired
    private PopMessageFetcher popMessageFetcher;

    @Override
    public MQProxyResponse<FetchResult> fetch(FetchRequest request) throws Exception {
        return getMessageFetcher(request).fetch(request);
    }

    @Override
    public MQProxyResponse<String> commitAck(FetchRequest request) throws Exception {
        return getMessageFetcher(request).commitAck(request);
    }

    public MessageFetcher getMessageFetcher(FetchRequest request) {
        return request.isPop() ? popMessageFetcher : pullMessageFetcher;
    }
}
