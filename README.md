# quarkus-rocketmq-client
测试案例可参考integration-tests中，其中消息消费者可定义的参数
列表如下：
- List&lt;MessageExt>
- ConsumeConcurrentlyContext / ConsumeOrderlyContext

返回参数如下之一：
- io.vertx.core.Future&lt;ConsumeConcurrentlyStatus>
- io.vertx.core.Future&lt;ConsumeOrderlyStatus>
- io.smallrye.mutiny.Uni&lt;ConsumeOrderlyStatus>
- io.smallrye.mutiny.Uni&lt;ConsumeConcurrentlyStatus>
- ConsumeConcurrentlyStatus
- ConsumeOrderlyStatus