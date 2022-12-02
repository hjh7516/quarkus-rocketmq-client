/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.rocketmq.client.store;

import static io.vertx.rocketmq.client.store.ReadOffsetType.READ_FROM_STORE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.rocketmq.client.common.ReactiveFile;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.log.ClientLogger;

/**
 * Local storage implementation
 */
public class LocalFileOffsetStore implements OffsetStore {
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
            "rocketmq.client.localOffsetStoreDir",
            System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static InternalLogger log = ClientLogger.getLog();
    private final String groupName;
    private final String storePath;
    private final ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();
    private final FileSystem fs;

    public LocalFileOffsetStore(FileSystem fs, String clientId, String groupName) {
        this.fs = fs;
        this.groupName = groupName;
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator + clientId + File.separator +
                this.groupName + File.separator + "offsets.json";
    }

    @Override
    public Future<Void> load() {
        Promise<Void> promise = Promise.promise();
        Future<OffsetSerializeWrapper> wrapperFuture = this.readLocalOffset();
        wrapperFuture.onFailure(promise::fail);
        wrapperFuture.onSuccess(offsetSerializeWrapper -> {
            if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

                for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                    AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                    log.info("load consumer's offset, {} {} {}",
                            this.groupName,
                            mq,
                            offset.get());
                }
            }
        });
        return promise.future();
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    @Override
    public Future<Long> readOffset(final MessageQueue mq, final ReadOffsetType type) {
        Promise<Long> promise = Promise.promise();
        if (mq != null) {
            if (READ_FROM_STORE.equals(type)) {
                Future<OffsetSerializeWrapper> wrapperFuture = this.readLocalOffset();
                wrapperFuture.onFailure(e -> promise.complete(-1L));
                wrapperFuture.onSuccess(offsetSerializeWrapper -> {
                    AtomicLong offset = null;
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateOffset(mq, offset.get(), false);
                        }
                    }
                    promise.complete(Optional.ofNullable(offset).map(AtomicLong::get).orElse(-1L));
                });
            } else {
                promise.complete(Optional.ofNullable(this.offsetTable.get(mq)).map(AtomicLong::get).orElse(-1L));
            }
        } else {
            promise.complete(-1L);
        }
        return promise.future();
    }

    @Override
    public Future<Void> persistAll(Set<MessageQueue> mqs) {
        Promise<Void> promise = Promise.promise();
        if (null == mqs || mqs.isEmpty()) {
            promise.complete();
            return promise.future();
        }

        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                AtomicLong offset = entry.getValue();
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            Future<Void> voidFuture = new ReactiveFile(fs).string2File(jsonString, this.storePath);
            voidFuture.onFailure(e -> {
                if (e instanceof IOException) {
                    log.error("persistAll consumer offset Exception, " + this.storePath, e);
                } else {
                    promise.fail(e);
                }
            });
            voidFuture.onSuccess(promise::complete);
        } else {
            promise.complete();
        }

        return promise.future();
    }

    @Override
    public Future<Void> persist(MessageQueue mq) {
        Promise<Void> promise = Promise.promise();
        promise.complete();
        return promise.future();
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public Future<Void> updateConsumeOffsetToBroker(final MessageQueue mq, final long offset, final boolean isOneway) {
        Promise<Void> promise = Promise.promise();
        promise.complete();
        return promise.future();
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    private Future<OffsetSerializeWrapper> readLocalOffset() {
        Promise<OffsetSerializeWrapper> promise = Promise.promise();
        Future<Boolean> existsFuture = fs.exists(storePath);
        existsFuture.onComplete(ar -> {
            if (!ar.succeeded() || !ar.result()) {
                promise.fail(new MQClientException("File is not exist or open fail", ar.cause()));
                return;
            }
            Future<Buffer> bufferFuture = fs.readFile(storePath);
            bufferFuture.onFailure(promise::fail);
            bufferFuture.onSuccess(buf -> {
                String content = buf.toString(Charset.defaultCharset());
                if (null == content || content.length() == 0) {
                    this.readLocalOffsetBak().onFailure(promise::fail).onSuccess(promise::complete);
                } else {
                    try {
                        promise.complete(OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class));
                    } catch (Exception e) {
                        log.warn("readLocalOffset Exception, and try to correct", e);
                        this.readLocalOffsetBak().onFailure(promise::fail).onSuccess(promise::complete);
                    }
                }
            });
        });
        return promise.future();
    }

    private Future<OffsetSerializeWrapper> readLocalOffsetBak() {
        Promise<OffsetSerializeWrapper> promise = Promise.promise();
        String filePath = this.storePath + ".bak";
        Future<Boolean> exists = fs.exists(filePath);
        exists.onFailure(promise::fail);
        exists.onSuccess(bool -> {
            if (!bool) {
                promise.complete();
                return;
            }

            Future<Buffer> readFile = fs.readFile(filePath);
            readFile.onFailure(promise::fail);
            readFile.onSuccess(buf -> {
                String content = buf.toString(Charset.defaultCharset());
                if (!(content != null && content.length() > 0)) {
                    promise.complete();
                    return;
                }

                try {
                    promise.complete(OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class));
                } catch (Exception e) {
                    log.warn("readLocalOffset Exception", e);
                    promise.fail(new MQClientException("readLocalOffset Exception, maybe fastjson version too low"
                            + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION), e));
                }
            });
        });

        return promise.future();
    }
}
