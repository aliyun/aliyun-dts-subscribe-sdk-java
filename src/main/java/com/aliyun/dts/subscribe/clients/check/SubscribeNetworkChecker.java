package com.aliyun.dts.subscribe.clients.check;

import com.aliyun.dts.subscribe.clients.check.util.NetUtil;
import com.aliyun.dts.subscribe.clients.common.RetryUtil;

import java.net.SocketException;
import java.util.concurrent.TimeUnit;

public class SubscribeNetworkChecker implements SubscribeChecker {

    private String brokerUrl;

    private RetryUtil retryUtil;

    public SubscribeNetworkChecker(String brokerUrl) {
        this.brokerUrl = brokerUrl;
        retryUtil = new RetryUtil(4, TimeUnit.SECONDS, 15, (e) -> true);
    }

    public CheckResult check()  {
        boolean isOk = true;
        String errMsg = null;

        try {
            retryUtil.callFunctionWithRetry(
                    () -> {
                        int index = brokerUrl.lastIndexOf(":");
                        String url = brokerUrl.substring(0, index);
                        int port = Integer.parseInt(brokerUrl.substring(index+1));
                        NetUtil.testSocket(url, port);
                    }
            );
        }  catch (Exception e) {
            isOk = false;
            errMsg = "telnet " + brokerUrl + " failed, please check the network and if the brokerUrl is correct";
        }

        return new CheckResult(isOk, errMsg);
    }
}
