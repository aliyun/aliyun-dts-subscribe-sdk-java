package com.taobao.drc.togo.util;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by longxuan on 2018/8/20.
 */
public class TopicAliasMeta {
    private static final Logger logger = LoggerFactory.getLogger(TopicAliasMeta.class);
    public static final TopicAliasMeta UNUSED_TOPIC_ALIAS_META = new TopicAliasMeta(null);
    public static final String PHY_TOPIC_NAME = "topicPhy";
    public static final String ALIAS_TOPIC_NAME = "topicAlias";
    private Map<String, String> topicAliasMap;
    private Map<String, String> phyToAliasMap;
    private String originContent;
    private boolean isTopicAliasEnabled = true;

    public TopicAliasMeta(String content) {
        this.originContent = content;
        topicAliasInit(this.originContent);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(128);
        stringBuilder.append("{ isTopicAliasEnabled [").append(isTopicAliasEnabled).append("], aliasMap [")
                .append(StringUtils.join(topicAliasMap)).append("]}");
        return stringBuilder.toString();
    }

    /**
     * Alias content string describe the alias reflect relationship.
     * It's content like [{"phyName":"topicA", "alias":"hh"}, {"phyName": "topicB", "alias":"yy"}]
     *
     * @param aliasContent
     * @return map contains alias to topic name
     */
    private  void topicAliasInit(String aliasContent) {
        if (StringUtils.isEmpty(aliasContent)) {
            isTopicAliasEnabled = false;
            topicAliasMap = Collections.emptyMap();
            return;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode jsonNode = objectMapper.readTree(aliasContent);
            if (!jsonNode.isArray()) {
                throw new RuntimeException("TopicAliasMeta: parse content [" + aliasContent + "] failed, array type required");
            }
            topicAliasMap = new HashMap<>();
            phyToAliasMap = new HashMap<>();
            for (JsonNode arrayNode: jsonNode) {
                String phyName = arrayNode.get(PHY_TOPIC_NAME).asText();
                String alias = arrayNode.get(ALIAS_TOPIC_NAME).asText();
                topicAliasMap.put(alias, phyName);
                phyToAliasMap.put(phyName, alias);
            }
        } catch (Exception e) {
            logger.error("TopicAliasMeta: parse content [" + aliasContent + "] failed", e);
            throw new RuntimeException("TopicAliasMeta: parse content [" + aliasContent + "] failed", e);
        }
        if (topicAliasMap.size() != phyToAliasMap.size()) {
            throw new RuntimeException("TopicAliasMeta: invalid content [" + aliasContent + "]");
        }
        if (topicAliasMap.isEmpty()) {
            isTopicAliasEnabled = false;
        }
    }


    public boolean isTopicAliasEnabled() {
        return isTopicAliasEnabled;
    }

    public String getPhyNameForAlias(String alias) {
        if (isTopicAliasEnabled) {
            return topicAliasMap.getOrDefault(alias, alias);
        } else {
            return alias;
        }
    }

    public String getAliasForPhyTopic(String phyTopic) {
        if (isTopicAliasEnabled) {
            return phyToAliasMap.getOrDefault(phyTopic, phyTopic);
        } else {
            return phyTopic;
        }
    }

}
