package com.aliyun.dts.subscribe.clients.check;

import com.aliyun.dts.subscribe.clients.check.util.NetUtil;

import java.net.SocketException;

public class SubscribeNetworkChecker implements SubscribeChecker {

    private String brokerUrl;

    public SubscribeNetworkChecker(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public CheckResult check() {
        boolean isOk = true;
        String errMsg = null;

        int index = brokerUrl.lastIndexOf(":");
        String url = brokerUrl.substring(0, index);
        int port = Integer.parseInt(brokerUrl.substring(index+1));

        try {
            NetUtil.testSocket(url, port);
        } catch (SocketException e) {
            isOk = false;
            errMsg = "telnet " + brokerUrl + " failed, please check the network and if the brokerUrl is correct";
        }

        return new CheckResult(isOk, errMsg);
    }
}
