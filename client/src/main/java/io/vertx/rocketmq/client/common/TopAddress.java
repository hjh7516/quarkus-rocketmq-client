package io.vertx.rocketmq.client.common;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

public class TopAddress {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final String wsAddr;
    private final String unitName;
    private final VertxInternal vertx;
    private final WebClient client;

    public TopAddress(final String wsAddr, VertxInternal vertx) {
        this(vertx, wsAddr, null);
    }

    public TopAddress(VertxInternal vertx, final String wsAddr, final String unitName) {
        this.wsAddr = wsAddr;
        this.unitName = unitName;
        this.vertx = vertx;
        client = WebClient.create(vertx);
    }

    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }

    public final Future<String> fetchNSAddr(long timeoutMills) {
        String url = this.wsAddr;
        if (!UtilAll.isBlank(this.unitName)) {
            url = url + "-" + this.unitName + "?nofix=1";
        }
        PromiseInternal<String> promise = vertx.promise();
        String finalUrl = url;
        client.get(url).as(BodyCodec.json(String.class)).timeout(timeoutMills)
                .send().onComplete(ar -> {
                    if (ar.failed()) {
                        log.error("fetch name server address exception", ar.cause());
                        promise.complete();
                    } else {
                        HttpResponse<String> result = ar.result();
                        if (200 == result.statusCode()) {
                            String body = result.body();
                            String line = clearNewLine(body);
                            promise.complete(line);
                        } else {
                            log.error("fetch nameserver address is null");
                            promise.complete();
                        }
                    }
                    String errorMsg = "connect to " + finalUrl + " failed, maybe the domain name " + MixAll.getWSAddr()
                            + " not bind in /etc/hosts";
                    errorMsg += FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL);
                    log.warn(errorMsg);
                });

        return promise.future();
    }
}
