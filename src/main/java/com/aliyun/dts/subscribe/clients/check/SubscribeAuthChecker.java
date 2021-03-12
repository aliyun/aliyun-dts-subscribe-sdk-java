package com.aliyun.dts.subscribe.clients.check;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.check.util.NetUtil;
import com.aliyun.dts.subscribe.clients.common.Util;
import com.taobao.drc.togo.util.NodeCommandClient;
import com.taobao.drc.togo.util.NodeCommandClientConfig;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketException;
import java.util.Collection;
import java.util.Properties;

public class SubscribeAuthChecker implements SubscribeChecker {
    private static final Logger LOG = LoggerFactory.getLogger(SubscribeAuthChecker.class);

    private ConsumerContext consumerContext;

    private Properties subscribeProperties;

    private NodeCommandClient.CommandClient commandClient;

    public SubscribeAuthChecker(ConsumerContext consumerContext) {
        this.consumerContext = consumerContext;
        this.subscribeProperties = consumerContext.getKafkaProperties();
    }

    @Override
    public CheckResult check() {
        boolean isOk = true;
        String errMsg = null;

        Properties consumerConfig = new Properties();
        Util.mergeSourceKafkaProperties(subscribeProperties, consumerConfig);

        try {

            String proxyUrl = consumerContext.getBrokerUrl();

            NodeCommandClientConfig nodeCommandClientConfig = new NodeCommandClientConfig(consumerConfig);

            NodeCommandClient.CommandClientConfig config = NodeCommandClient.CommandClientConfig.fromNodeCommandClientConfig(nodeCommandClientConfig);

            this.commandClient = new NodeCommandClient.CommandClient(config);

            MetadataResponse metadataResponse = commandClient.fetchMetadata();

            Collection<Node> nodes = metadataResponse.brokers();

            for (Node node : nodes) {

                if (!proxyUrl.equalsIgnoreCase(node.host() + ":" + node.port())) {
                    LOG.info("Real broker node : " + node);
                }

                try {
                    NetUtil.testSocket(node.host(), node.port());
                } catch (SocketException e) {
                    isOk = false;
                    errMsg = "telnet real node " + node.toString() + " failed, please check the network";

                    return new CheckResult(isOk, errMsg);
                }
            }
        } catch (Exception e) {
            isOk = false;
            errMsg = "build kafka consumer failed, error: " + e + ", probably the user name or password is wrong";

            return new CheckResult(isOk, errMsg);
        }

        return new CheckResult(isOk, errMsg);
    }
}
