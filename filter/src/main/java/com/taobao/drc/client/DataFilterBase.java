package com.taobao.drc.client;

import com.taobao.drc.client.enums.DBType;

import java.util.List;
import java.util.Map;

/**
 * Created by longxuan on 17/2/4.
 * longpeng.zlp@alibaba-inc.com
 * Base implement define for data filter.
 * Notice: Data filter is not thread safe, and multi client instance should not share one data filter
 * even the filter string is the same.
 */
public interface DataFilterBase {
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
     * @param dbType database type which may be ob, mysql, oracle.
     * For now, only ob1.0 need special handle which 4 tuple contains tenant, db, tb, cols is strictly required.
     * @return true if filter is valid
     */
    public  boolean validateFilter(DBType dbType) throws DRCClientException;

    /**
     * This function is  compatible for old usage.
     * @param branchDb
     */
    public  void setBranchDb(String branchDb);

    /**
     * Fast match if cols are all needed.
     * @return true if all cols are needed
     */
    public  boolean getIsAllMatch();


    public  Map<String, Map<String, List<String>>> getReflectionMap();
    public  Map<String, Map<String, List<String>>> getRequireMap();
    Map<String, Map<String, List<String>>> dstoreRequiredMap();

}
