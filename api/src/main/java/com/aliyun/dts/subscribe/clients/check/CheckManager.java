package com.aliyun.dts.subscribe.clients.check;

public interface CheckManager {

    void addCheckItem(SubscribeChecker subscribeChecker);

    CheckResult check();
}
