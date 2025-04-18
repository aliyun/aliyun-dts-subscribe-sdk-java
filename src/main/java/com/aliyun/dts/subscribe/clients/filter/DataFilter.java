package com.aliyun.dts.subscribe.clients.filter;

import java.util.List;
import java.util.Map;

public interface DataFilter {
    public final String FILTER_SEPARATOR_INNER = ".";
    public final String FILTER_SEPARATOR = "|";

    /**
     * Get the formatted filter string which will be delivered to store.
     * Notice: before validate filter function called, getConnectStoreFilterConditions may return null.
     * @return filter string
     */
    public  String getConnectStoreFilterConditions();

    /**
     * Validate if the filter user passed is legal
     * For now, only ob1.0 need special handle which 4 tuple contains tenant, db, tb, cols is strictly required.
     * @return true if filter is valid
     */
    public  boolean validateFilter();

    /**
     * Fast match if cols are all needed.
     * @return true if all cols are needed
     */
    public  boolean getIsAllMatch();

    public  Map<String, Map<String, List<String>>> getReflectionMap();
    public  Map<String, Map<String, List<String>>> getRequireMap();
    Map<String, Map<String, List<String>>> dstoreRequiredMap();

    String getBlackList();

    void setBlackList(String blackList);
}
