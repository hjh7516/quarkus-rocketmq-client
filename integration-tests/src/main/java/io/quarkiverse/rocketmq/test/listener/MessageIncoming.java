package io.quarkiverse.rocketmq.test.listener;

import com.alibaba.fastjson.JSON;
import io.quarkiverse.rocketmq.client.runtime.RocketmqIncoming;
import io.smallrye.mutiny.Uni;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;

@Slf4j
@ApplicationScoped
public class MessageIncoming {

    @RocketmqIncoming(configKey = "product")
    public Uni<ConsumeConcurrentlyStatus> process(List<MessageExt> messageExts, ConsumeConcurrentlyContext context) {
        log.info("Normal MessageIncoming[product] content is {}, context: {}", new String(messageExts.get(0).getBody()), JSON.toJSONString(context));
        return Uni.createFrom().item(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
    }

    @RocketmqIncoming(configKey = "user")
    public ConsumeConcurrentlyStatus process(List<MessageExt> messageExts) {
        log.info("Normal MessageIncoming[user] content is {}", new String(messageExts.get(0).getBody()));
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
