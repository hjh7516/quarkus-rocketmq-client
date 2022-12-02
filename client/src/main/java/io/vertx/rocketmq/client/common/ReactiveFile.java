package io.vertx.rocketmq.client.common;

import java.nio.charset.Charset;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;

public class ReactiveFile {

    private final FileSystem fs;

    public ReactiveFile(FileSystem fs) {
        this.fs = fs;
    }

    public Future<Void> string2File(String str, String fileName) {
        Promise<Void> promise = Promise.promise();
        String tmpFile = fileName + ".tmp";
        Future<Void> voidFuture = string2FileNotSafe(str, tmpFile);
        voidFuture.onFailure(promise::fail);
        voidFuture.onSuccess(v -> {
            String bakFile = fileName + ".bak";
            Future<String> prevContentFuture = file2String(fileName);
            prevContentFuture.onFailure(promise::fail);
            prevContentFuture.onSuccess(prevContent -> {
                Promise<Void> toFilePromise = Promise.promise();
                if (prevContent != null) {
                    string2FileNotSafe(prevContent, bakFile)
                            .onFailure(toFilePromise::fail)
                            .onSuccess(toFilePromise::complete);
                } else {
                    toFilePromise.complete();
                }

                toFilePromise.future().onComplete(ar -> {
                    if (ar.failed()) {
                        promise.fail(ar.cause());
                    } else {
                        Future<Void> delete = fs.delete(fileName);
                        delete.onFailure(promise::fail);
                        delete.onSuccess(
                                v1 -> fs.move(tmpFile, fileName).onFailure(promise::fail).onSuccess(promise::complete));
                    }
                });
            });
        });

        return promise.future();
    }

    public Future<Void> string2FileNotSafe(String str, String fileName) {
        Promise<Void> promise = Promise.promise();
        Future<Boolean> exists = fs.exists(fileName);
        exists.onFailure(promise::fail);
        exists.onSuccess(bool -> {
            Promise<Void> toWrite = Promise.promise();
            if (!bool) {
                Future<Void> file = fs.createFile(fileName);
                file.onFailure(promise::fail);
                file.onSuccess(toWrite::complete);
            } else {
                toWrite.complete();
            }

            toWrite.future().onSuccess(v -> {
                Future<Void> voidFuture = fs.writeFile(fileName, Buffer.buffer(str));
                voidFuture.onFailure(promise::fail);
                voidFuture.onSuccess(promise::complete);
            });
        });

        return promise.future();
    }

    public Future<String> file2String(String fileName) {
        Promise<String> promise = Promise.promise();
        Future<Boolean> exists = fs.exists(fileName);
        exists.onFailure(promise::fail);
        exists.onSuccess(bool -> {
            if (!bool) {
                promise.complete();
            } else {
                Future<Buffer> readFile = fs.readFile(fileName);
                readFile.onFailure(promise::fail);
                readFile.onSuccess(buf -> promise.complete(buf.toString(Charset.defaultCharset())));
            }
        });

        return promise.future();
    }
}
