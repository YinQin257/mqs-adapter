package org.yinqin.mqs.common.entity;

public interface MessageCallback {

    void onSuccess();

    void onError(Throwable throwable);
}
