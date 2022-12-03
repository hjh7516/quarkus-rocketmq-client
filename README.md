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
##### 由于没有打入到任何的maven仓库中，所以有需要的开发者可以下载到自己的本地上，建议下载master分支，因为这个分支都是经过开发分支修复bug以后合并过来的
