package io.quarkiverse.rocketmq.test.listener;

import com.alibaba.fastjson.JSON;
import io.quarkiverse.rocketmq.client.runtime.RocketmqIncoming;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;

@Slf4j
@ApplicationScoped
public class OrderMessageIncoming {

    @RocketmqIncoming(configKey = "order", isOrderly = true)
    public Future<ConsumeOrderlyStatus> process(List<MessageExt> messageExts, ConsumeOrderlyContext context) {
        log.info("OrderMessage, message[order] content is {}, context : {}", new String(messageExts.get(0).getBody()), JSON.toJSONString(context));

        Promise<ConsumeOrderlyStatus> promise = Promise.promise();
        promise.complete(ConsumeOrderlyStatus.SUCCESS);
        return promise.future();
    }

    @Data
    static class User {
        private int age;
        private String name;

    }
}
