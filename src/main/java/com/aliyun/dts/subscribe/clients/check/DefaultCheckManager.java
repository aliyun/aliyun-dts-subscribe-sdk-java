package com.aliyun.dts.subscribe.clients.check;

import com.aliyun.dts.subscribe.clients.ConsumerContext;

import java.util.ArrayList;
import java.util.List;

public class DefaultCheckManager implements CheckManager {

    private ConsumerContext consumerContext;

    private List<SubscribeChecker> checkerList;

    public DefaultCheckManager(ConsumerContext consumerContext) {
        this.consumerContext = consumerContext;

        checkerList = new ArrayList<>();
    }

    @Override
    public void addCheckItem(SubscribeChecker subscribeChecker) {
        checkerList.add(subscribeChecker);
    }

    @Override
    public CheckResult check() {
        CheckResult checkResult = CheckResult.SUCESS;
        for(SubscribeChecker checker : checkerList) {
            checkResult = checker.check();

            if(!checkResult.isOk()) {
                return checkResult;
            }
        }

        return checkResult;
    }
}
