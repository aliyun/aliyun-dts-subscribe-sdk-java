package com.taobao.drc.client.cm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taobao.drc.client.DRCClientException;
import com.taobao.drc.client.SubscribeChannel;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.NameValuePair;
import org.apache.http.client.fluent.Request;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * cluster manager facade
 * Created by jianjun.djj on 10/1/14.
 */
public class ClusterManagerFacade {

    private static final Log log = LogFactory.getLog(ClusterManagerFacade.class);

    private static final int timeout = 60000;

    public static final String DEFAULT_URL_ENCODING = "UTF-8";

    public static final String KEY_IS_SUCCESS = "isSuccess";

    public static final String KEY_ERROR_CODE = "errCode";

    public static final String KEY_ERROR_MESSAGE = "errMsg";

    /**
     * ask token
     *
     * @param userConfig
     * @return
     * @throws IOException
     */
    public static void askToken(UserConfig userConfig) throws Exception {
        String url = String.format("%s/auth?consumer=%s&password=%s", userConfig.getChannelUrl(), URLEncoder.encode(userConfig.getUserName(), DEFAULT_URL_ENCODING), URLEncoder.encode(userConfig.getPassword(), DEFAULT_URL_ENCODING));
        String result = Request.Get(url).socketTimeout(timeout).execute().returnContent().asString();
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getBoolean(KEY_IS_SUCCESS)) {
            userConfig.setToken(jsonObject.getString("token"));
        } else {
            throw new RoutingException("Failed to auth with [" + userConfig.getUserName() + "][" + userConfig.getPassword() + "]: [" + result + "]");
        }
    }

    public static void askTopic(UserConfig userConfig) throws Exception {
        Pair<JSONArray, String> topicData = askTopicData(userConfig);
        JSONArray jsonArray = topicData.getLeft();
        if (jsonArray.size() > 1) {
            throw new RoutingException("Found multiple than one topics with db name[" + userConfig.getDb() + "]: [" + topicData.getRight() + "]");
        } else {
            userConfig.setSubTopic(String.valueOf(jsonArray.get(0)));
        }
    }

    public static List<String> askMultiTopic(UserConfig userConfig) throws Exception {
        ArrayList<String> list = new ArrayList<String>();
        JSONArray jsonArray = askTopicData(userConfig).getLeft();
        for (Object object : jsonArray) {
            list.add(String.valueOf(object));
        }
        return list;
    }

    private static Pair<JSONArray, String> askTopicData(UserConfig userConfig) throws Exception {
        String url = String.format("%s/list/%s", userConfig.getChannelUrl(), userConfig.getDb());
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("token", userConfig.getToken()));
        String result = Request.Post(url).bodyForm(params, Charset.forName(DEFAULT_URL_ENCODING)).socketTimeout(timeout).execute().returnContent().asString();
        JSONObject jsonObject = JSON.parseObject(result);
        if (!jsonObject.getBoolean(KEY_IS_SUCCESS)) {
            throw new RoutingException("Failed to list topic with db name[" + userConfig.getDb() + "]: [" + result + "]");
        }
        return ImmutablePair.of(jsonObject.getJSONArray("topics"), result);
    }

    /**
     * ask store
     *
     * @param userConfig
     * @return
     * @throws IOException
     */
    public static String askStore(UserConfig userConfig) throws Exception {
        String url = String.format("%s/storehost/%s", userConfig.getChannelUrl(), userConfig.getSubTopic());

        String filterStr = URLEncoder.encode(userConfig.getDataFilter().toString(), "ISO-8859-1");

        List<NameValuePair> params = new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("token",userConfig.getToken()));
        params.add(new BasicNameValuePair("checkpoint",userConfig.getCheckpoint().toString()));
        params.add(new BasicNameValuePair("filter", filterStr));
        params.add(new BasicNameValuePair("publicIp",String.valueOf(userConfig.isUsePublicIp())));
        String result = Request.Post(url).bodyForm(params, Charset.forName(DEFAULT_URL_ENCODING)).socketTimeout(timeout).execute().returnContent().asString();
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getBoolean(KEY_IS_SUCCESS)) {
            userConfig.setStore(jsonObject.getString("id"));
            return jsonObject.getString("store");
        }
        throw new RoutingException("Failed to find store for [" + userConfig.getSubTopic() + "] with checkpoint [" + userConfig.getCheckpoint()
                + "] and filter [" + filterStr + "]: [" + result + "]");
    }

    public static StoreInfo fetchStoreInfo(UserConfig userConfig, boolean useDRCNet) throws Exception {
        String storeAddr = askStore(userConfig);
        String parts[] = StringUtils.split(storeAddr, ':');
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        int drcNetPort;
        if (useDRCNet) {
            drcNetPort = askDataPort(host, parts[1], userConfig);
        } else {
            drcNetPort = 0;
        }
        return new StoreInfo(host, port, drcNetPort);
    }

    /**
     * ask drc data port
     *
     * @param userConfig
     * @return
     * @throws IOException
     */
    public static int askDataPort(String host, String port, UserConfig userConfig) throws Exception {
        String uri = "http://" + host + ":" + port + "/" + userConfig.getSubTopic();
        List<NameValuePair> params = new ArrayList<NameValuePair>();
        Map<String, String> paramMap = userConfig.getParams();
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        params.add(new BasicNameValuePair("useDrcNet", "true"));
        byte[] data = Request.Post(uri).bodyForm(params, Charset.forName(DEFAULT_URL_ENCODING)).socketTimeout(timeout).execute().returnContent().asBytes();
        return CommonUtils.byteToInt(data);
    }

    /**
     * ask region info
     *
     * @param userConfig
     * @return
     * @throws IOException
     */
    public static void askRegionInfo(UserConfig userConfig) {
        String url = String.format("%s/region/id?subTopic=%s", userConfig.getChannelUrl(), userConfig.getSubTopic());
        try {
            String result = Request.Get(url).socketTimeout(timeout).execute().returnContent().asString();
            JSONObject jsonObject = JSON.parseObject(result);
            if (jsonObject.getBoolean(KEY_IS_SUCCESS)) {
                String regionId = jsonObject.getString("regionId");
                log.warn("Region id: " + regionId);
                userConfig.setRegionInfo(regionId);
            }
        } catch (IOException e) {
            log.warn("ask region info failed");
        }
    }

    public static DBType askTopicType(UserConfig userConfig, String topic) throws IOException, DRCClientException {
        String composedUrl = userConfig.getChannelUrl() + "/topic/query" + "?topic=" + URLEncoder.encode(topic, DEFAULT_URL_ENCODING);

        String result = Request.Get(composedUrl).socketTimeout(timeout).execute().returnContent().asString();
        JSONObject jsonObject = JSON.parseObject(result);
        if (!jsonObject.getBoolean(KEY_IS_SUCCESS)) {
            throw new DRCClientException(jsonObject.getInteger(KEY_ERROR_CODE) + ":" + jsonObject.getString(KEY_ERROR_MESSAGE));
        }
        String type = jsonObject.getString("type");
        return CommonUtils.getDatabaseType(type);
    }

    public static class StoreInfo {
        private final String host;
        private final int port;
        private final int drcNetPort;

        public StoreInfo(String host, int port, int drcNetPort) {
            this.host = host;
            this.port = port;
            this.drcNetPort = drcNetPort;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public int getDrcNetPort() {
            return drcNetPort;
        }
    }

    public static class RoutingException extends Exception {
        public RoutingException(String message) {
            super(message);
        }
    }

    /**
     * ask subscribe channel type
     * @param userConfig
     */
    public static SubscribeChannel askSubscribeChannelAndRedirect(UserConfig userConfig) {
        String url = String.format("%s/client/switch/verify", userConfig.getClusterUrl());
        try {
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            params.add(new BasicNameValuePair("queryKey", userConfig.getDb()));
            params.add(new BasicNameValuePair("regionCode", StringUtils.trimToEmpty(userConfig.getRegionCode())));
            params.add(new BasicNameValuePair("consumer", userConfig.getUserName()));
            params.add(new BasicNameValuePair("reversed", StringUtils.trimToEmpty(userConfig.getReversed())));
            params.add(new BasicNameValuePair("ip", StringUtils.trimToEmpty(userConfig.getClientIp())));
            params.add(new BasicNameValuePair("__skipAuth", userConfig.isSkipAuth() + ""));
            log.info(url + " " + params);
            String result = Request.Post(url).bodyForm(params).socketTimeout(timeout).execute().returnContent().asString();
            log.info(url + "    ->    " + result);
            JSONObject jsonObject = JSON.parseObject(result);
            if (jsonObject.getBoolean(KEY_IS_SUCCESS)) {
                String redirectUrl = jsonObject.getString("redirectUrl");
                if (StringUtils.isNotEmpty(redirectUrl)) {
                    log.warn("redirectUrl is not null, redirect from [" + userConfig.getClusterUrl() + "] to [" + redirectUrl + "]");
                    userConfig.setClusterUrl(redirectUrl);
                }

                JSONObject data = jsonObject.getJSONObject("data");
                return SubscribeChannel.valueBy(data.getString("taskType"));
            } else {
                log.error("ask subscription channel failed:" + StringUtils.trimToEmpty(jsonObject.getString("msg")));
            }
        } catch (Throwable e) {
            log.warn("ask subscription channel failed:" + e.getMessage());
        }
        return SubscribeChannel.DRC;
    }

    /**
     * ask for cm url
     * @param userConfig
     */
    public static void askForCmUrl(UserConfig userConfig, SubscribeChannel channel) {
        String url = String.format("%s/client/switch/address", userConfig.getClusterUrl());
        try {
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            params.add(new BasicNameValuePair("regionCode", StringUtils.trimToEmpty(userConfig.getRegionCode())));
            params.add(new BasicNameValuePair("taskType", userConfig.getSubscribeChannel().name().toLowerCase()));
            params.add(new BasicNameValuePair("__skipAuth", userConfig.isSkipAuth() + ""));
            log.info(url + " " + params);
            String result = Request.Post(url).bodyForm(params).socketTimeout(timeout).execute().returnContent().asString();
            log.info(url + "    ->    " + result);
            JSONObject jsonObject = JSON.parseObject(result);
            if (jsonObject.getBoolean(KEY_IS_SUCCESS)) {
                JSONObject data = jsonObject.getJSONObject("data");
                userConfig.setChannelUrl(data.getString("serverAddr"));
                userConfig.setRegionInfo(data.getString("regionId"));
            } else {
                log.error("ask for cm failed:" + StringUtils.trimToEmpty(jsonObject.getString("msg")));
            }
        } catch (Throwable e) {
            log.error("ask for cm failed:" + e.getMessage());
        }
    }

    /**
     * ask for DStore topics
     * @param userConfig
     * @return
     */
    public static String[] askForDStoreTopic(UserConfig userConfig) {
        String url = String.format("%s/client/switch/topic/list", userConfig.getClusterUrl());
        try {
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            params.add(new BasicNameValuePair("queryKey", userConfig.getDb()));
            params.add(new BasicNameValuePair("taskType", userConfig.getSubscribeChannel().name().toLowerCase()));
            params.add(new BasicNameValuePair("regionCode", StringUtils.trimToEmpty(userConfig.getRegionCode())));
            params.add(new BasicNameValuePair("__skipAuth", userConfig.isSkipAuth() + ""));
            params.add(new BasicNameValuePair("consumer", StringUtils.trimToEmpty(userConfig.getUserName())));
            log.info(url + " " + params);
            String result = Request.Post(url).bodyForm(params).socketTimeout(timeout).execute().returnContent().asString();
            log.info(url + "    ->    " + result);
            JSONObject jsonObject = JSON.parseObject(result);
            if (jsonObject.getBoolean(KEY_IS_SUCCESS)) {
                JSONObject data = jsonObject.getJSONObject("data");
                JSONArray jsonArrayTopics = data.getJSONArray("topics");

                //set regionId
                String regionId = data.getString("regionId");
                log.info("Got region id: " + regionId);
                userConfig.setRegionInfo(regionId);

                return jsonArrayTopics.toArray(new String[0]);
            } else {
                log.error("ask for DStore topics failed:" + StringUtils.trimToEmpty(jsonObject.getString("msg")));
                throw new RuntimeException(StringUtils.isEmpty(jsonObject.getString("msg")) ? result : jsonObject.getString("msg"));
            }
        } catch (Throwable e) {
            log.error("ask for DStore topics failed:" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * enroll  DStore channel
     * @param userConfig
     * @return channelId
     */
    public static String enrollDStoreChannel(UserConfig userConfig) {
        String url = String.format("%s/client/switch/channel/register", userConfig.getClusterUrl());
        try {
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            params.add(new BasicNameValuePair("taskType", userConfig.getChannel().name().toLowerCase()));
            params.add(new BasicNameValuePair("topic", userConfig.getSubTopic()));
            params.add(new BasicNameValuePair("regionCode", StringUtils.trimToEmpty(userConfig.getRegionCode())));
            params.add(new BasicNameValuePair("consumer", userConfig.getUserName()));
            if (null != userConfig.getCheckpoint()) {
                params.add(new BasicNameValuePair("checkpoint", userConfig.getCheckpoint().getTimestamp()));
            }
            params.add(new BasicNameValuePair("__skipAuth", userConfig.isSkipAuth() + ""));
            log.info(url + " " + params);
            String result = Request.Post(url).bodyForm(params).socketTimeout(timeout).execute().returnContent().asString();
            log.info(url + "    ->    " + result);
            JSONObject jsonObject = JSON.parseObject(result);
            if (jsonObject.getBoolean(KEY_IS_SUCCESS)) {
                return jsonObject.getString("data");
            } else {
                log.error("ask for DStore topics failed:" + StringUtils.trimToEmpty(jsonObject.getString("msg")));
                throw new RuntimeException(StringUtils.isEmpty(jsonObject.getString("msg")) ? result : jsonObject.getString("msg"));
            }
        } catch (Throwable e) {
            log.error("ask for DStore topics failed:" + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
