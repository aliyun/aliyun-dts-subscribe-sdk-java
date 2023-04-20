package com.taobao.drc.togo.client.consumer;

import com.taobao.drc.togo.common.businesslogic.HashTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.SpecialTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.UnitTypeFilterTag;
import org.apache.kafka.common.requests.SetFetchTagsRequest;

import java.util.List;

/**
 * @author yangyang
 * @since 17/3/9
 */
public interface FetchRule {
    List<SetFetchTagsRequest.TagEntry> createFetchEntry();
    SpecialTypeFilterTag specialTypeFilterTag();
    HashTypeFilterTag hashTypeFilterTag();
    UnitTypeFilterTag unitTypeFilterTag();
}