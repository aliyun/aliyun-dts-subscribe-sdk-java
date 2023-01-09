package com.taobao.drc.client;

import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.utils.DataFilterUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataFilter contains table names and fields names which are requested.
 * The format is "tableName;fieldName;fieldName|tableName;fieldName".
 * Support wildcard character *, e.g., tableName auction_auction_0000
 * and auction_auction_0001 could be represented by auction_auction*.
 * Currently, only "*" is supported, which automatically matches _####
 * that is _ followed by four digits.
 * @author erbai.qzc
 *
 */
@Deprecated
public class DataFilter implements DataFilterBase{
    /* Used to be compatibility with old version */
    private String oldBranchDb;

    private String filterInfo;
    // String save the source filter string user passed through
    private String sourceFilter;
    // String save the filter that will be sent to store to acquire data
    // For ob1.0, that must be four columns, dbname like a.b.
    private String connectStoreFilterConditions;

    private final StringBuilder builder;

    private final Map<String, Map<String, List<String>>> requires;

    private final Map<String, Map<String, List<String>>> dbTableColsReflectionMap;
    //If all cols needed is '*', then we don't need do filter operation.
    //In that case, we can save a lot compute.
    private boolean isAllMatch = true;

    private String tenant;

    public DataFilter() {
        oldBranchDb = null;
        filterInfo = null;
        builder = new StringBuilder();
        requires = new HashMap<String, Map<String, List<String>>>();
        dbTableColsReflectionMap = new HashMap<String, Map<String, List<String>>>();
    }

    /**
     * Initialize the filter using formatted string.
     * @param tableFields the formatted filter information such as
     * "tableName1;fieldName1;fieldName2|tableName2;fieldName1". No ";" or "|" should be
     *  transfer to
     * *.tableName1.fieldName1|*.tableName1.fieldName2|...
     * added at the beginning or end of the string.
     */
    public DataFilter(String tenant, String tableFields) {
        this(tableFields);
        this.tenant=tenant;
    }

    public DataFilter(String tableFields){
        oldBranchDb = null;
        builder = new StringBuilder();
        requires = new HashMap<String, Map<String, List<String>>>();
        dbTableColsReflectionMap = new HashMap<String, Map<String, List<String>>>();
        builder.append(tableFields);
        this.sourceFilter = tableFields;
    }

    /**
     * The current version uses topic instead of dbname, so use the
     * method to be compatible with the older version.
     * @param db is the original branched db name.
     */
    public void setBranchDb(final String db) {
        oldBranchDb = db;
    }

    /**
     * Add more filter information after initializing, note that the user should
     * make it consistent to the formatted parameters.
     * @param tableFields consistent formatted filter information.
     */
    public void addTablesFields(String tableFields) {
        builder.append(tableFields);
    }

    public boolean getIsAllMatch() {
        return isAllMatch;
    }

    @Override
    public Map<String, Map<String, List<String>>> getReflectionMap() {
        return dbTableColsReflectionMap;
    }

    @Override
    public Map<String, Map<String, List<String>>> getRequireMap() {
        return requires;
    }


    //Before validate function called, toString may return null;
    //Yet, user should not care about this. That's inter behavior.
    @Override
    public String toString() {
        return connectStoreFilterConditions;
    }

    /**
     * The validate function will form mysql, ob0.5, oracle eg filter condition.
     */
    private boolean validateNormalFilterString() {
        if (filterInfo != null)
            return true;

        String s = builder.toString();
        String[] tbs = s.split("\\|");
        if (tbs == null) {
            return false;
        }

        int colStart;
        StringBuilder builder1 = new StringBuilder();
        for (String s1 : tbs) {
            String[] tb = s1.split("[;,\\.]");
            if (tb != null && tb.length > 0) {

                String itemDb;
                String itemTb;

                if (tb.length <= 2) {
                    if (oldBranchDb != null) {
                        itemDb = oldBranchDb;
                    } else {
                        itemDb = "*";
                    }
                    colStart = 1;
                    itemTb = tb[0];
                } else {
                    colStart = 2;
                    itemDb = tb[0];
                    itemTb = tb[1];
                }
                if(tenant!=null){
                    builder1.append(tenant).append(".");
                }
                builder1.append(itemDb).append(".").append(itemTb).append("|");
                if (colStart > 0 && tb.length > colStart) {
                    List<String> cols = new ArrayList<String>();
                    for (int i = colStart; i < tb.length; i ++) {
                        cols.add(tb[i]);
                        //here, we don't use trim in case that  " *" or "* " or " * " is kind of col names
                        if(!"*".equals(tb[i])) {
                            isAllMatch = false;
                        }
                    }

                    DataFilterUtil.putColNames(itemDb, itemTb, cols, this);
                }
            }
        }
        if (builder1.charAt(builder1.length() - 1) == '|')
            builder1.deleteCharAt(builder1.length() - 1);
        filterInfo = builder1.toString();
        connectStoreFilterConditions = filterInfo;
        return true;
    }

    /**
     * The validate function will reform the filter condition and cols info
     */
    private boolean validateOB10FilterString() {
        if (sourceFilter == null) {
            return false;
        }
        String[] tenantAndDbAndTBAndCols = sourceFilter.split("\\|");
        if (tenantAndDbAndTBAndCols == null) {
            return false;
        }
        requires.clear();
        StringBuilder filterConditionBuilder = new StringBuilder();
        for (String s1 : tenantAndDbAndTBAndCols) {
            String[] tb = s1.split("[;,\\.]");
            if (null == tb || tb.length < 4) {
                // tenant dbname tbname colnames is strictly required for 0b1.0
                return false;
            }
            String tenant = tb[0];
            String dbname =  (oldBranchDb != null) ? oldBranchDb : tb[1];
            String tableName = tb[2];
            List<String> cols = new ArrayList<String>();
            for (int i = 3; i < tb.length; ++i) {
                cols.add(tb[i]);
                if(!"*".equals(tb[i])) {
                    isAllMatch = false;
                }
            }
            //format string passed to store
            String formatDBName = tenant + "." + dbname;
            filterConditionBuilder.append(formatDBName).append(FILTER_SEPARATOR_INNER);
            filterConditionBuilder.append(tableName).append(FILTER_SEPARATOR);
            DataFilterUtil.putColNames(formatDBName, tableName, cols, this);
        }
        connectStoreFilterConditions = filterConditionBuilder.toString();
        return true;
    }

    // When source type is ocean base 1.0, filter's content is like tenant.dbname.tablename.colvalues| ....
    public boolean validateFilter(DBType dbType) throws DRCClientException{
        synchronized (this) {
            switch (dbType) {
                case OCEANBASE1: {
                    return validateOB10FilterString();
                }
                default: {
                    return validateNormalFilterString();
                }
            }
        }
    }

    @Override
    public String getConnectStoreFilterConditions() {
        return this.connectStoreFilterConditions;
    }

    @Override
    public Map<String, Map<String, List<String>>> dstoreRequiredMap() {
        Map<String, Map<String, List<String>>> requiredMap = new HashMap<String, Map<String, List<String>>>();
        String[] tbs = StringUtils.isNotEmpty(sourceFilter) ? sourceFilter.split("\\|") : new String[]{};
        for (String tb : tbs) {
            String[] dbAndTable = tb.split("[;,\\.]");
            if (dbAndTable.length > 1) {
                String db = dbAndTable[0];
                Map<String, List<String>> fieldList = requiredMap.get(db);
                if (null == fieldList) {
                    fieldList = new HashMap<String, List<String>>();
                }
                fieldList.put(dbAndTable[1], new ArrayList<String>(0));
                requiredMap.put(dbAndTable[0], fieldList);
            }
        }
        return requiredMap;
    }
}
