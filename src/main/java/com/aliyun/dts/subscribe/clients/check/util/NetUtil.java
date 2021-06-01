package com.aliyun.dts.subscribe.clients.check.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

public class NetUtil {
    public static boolean testSocket(String ip, int port) throws SocketException {
        Socket soc = null;
        try {
            soc = new Socket();
            soc.connect(new InetSocketAddress(ip, port), 5000);
            return true;
        } catch (Exception ex) {
            throw new SocketException(ex.getMessage());
        } finally {
            try {
                soc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
