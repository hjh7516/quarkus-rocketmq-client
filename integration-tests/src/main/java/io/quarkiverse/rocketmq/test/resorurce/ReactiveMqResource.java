package io.quarkiverse.rocketmq.test.resorurce;

import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQProducer;
import io.quarkiverse.rocketmq.test.resorurce.entity.BizResult;
import io.smallrye.mutiny.Uni;
import io.vertx.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.io.UnsupportedEncodingException;

@Path("/reactive/mq")
public class ReactiveMqResource {

    /**
     * 可以注入响应式api
     * */
    @Inject
    ReactiveMQProducer producer;

    @Path("/send")
    @GET
    public Uni<BizResult<SendResult>> send(@QueryParam("content") String content) throws UnsupportedEncodingException {
        Message message = new Message("reactive-mq", content.getBytes(RemotingHelper.DEFAULT_CHARSET));
        return producer.send(message).map(BizResult::create);
    }
}
