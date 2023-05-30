package com.taobao.drc.client;

import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.utils.DataFilterUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataFilterV2 implements DataFilterBase {
    private static final Logger logger = LoggerFactory.getLogger(DataFilterV2.class);

    /**
     * Class store the raw info user passed.
     */
    public class FilterInfo {
        private String tenant;
        private String dbName;
        private String tableName;
        private List<String> colsList;
        public FilterInfo(String tenant, String dbName, String tableName, String ... cols) {
            this.tenant = tenant;
            this.dbName = dbName;
            this.tableName = tableName;
            colsList = new LinkedList<String>();
            if (null != cols) {
                for (String col : cols) {
                    colsList.add(col);
                }
            }
        }

        public String getTenant() {
            return this.tenant;
        }
        public String getDbName() {
            return this.dbName;
        }
        public String getTableName() {
            return this.tableName;
        }
        public List<String> getColsList() {
            return this.colsList;
        }
    }

    private String branchDB;
    private List<FilterInfo> filterInfoList;
    private boolean isAllMatch;
    private String storeFilter;
    private AtomicBoolean haveValidated;
    // Still, we keep logical insist with old data filter  impl, use 2 map divide add op and find op.
    private Map<String, Map<String, List<String>>> requires;
    private Map<String, Map<String, List<String>>> dbTableColsReflectionMap;

    //parse the filterInfoList to requires
    private boolean hasParsedFilter = false;

    private DataFilterV2(String branchDB, List<FilterInfo> filterInfos) {
        if (null == filterInfos) {
            filterInfoList = new LinkedList<FilterInfo>();
        } else {
            filterInfoList = filterInfos;
        }
        this.branchDB = branchDB;
        haveValidated = new AtomicBoolean(false);
        requires = new HashMap<String, Map<String, List<String>>>();
        dbTableColsReflectionMap = new HashMap<String, Map<String, List<String>>>();
        isAllMatch = true;
        storeFilter = null;
    }

    private DataFilterV2() {
        this(null, null);
    }

    public static DataFilterV2 create() {
        return new DataFilterV2();
    }

//    public static DataFilterBaseImpl create(DBType dbType) {
//    }

    public DataFilterV2 addFilterTuple(String tenant, String db, String table, String ...cols) {
        FilterInfo filterInfo = new FilterInfo(tenant, db, table, cols);
        this.filterInfoList.add(filterInfo);
        return this;
    }


    public List<FilterInfo> getFilterInfoList() {
        return this.filterInfoList;
    }

    public String getBranchDB() {
        return this.branchDB;
    }

    @Override
    public String getConnectStoreFilterConditions() {
        return storeFilter;
    }

    @Override
    public boolean validateFilter(DBType dbType) throws DRCClientException {
        synchronized (this) {
            if (!haveValidated.compareAndSet(false, true)) {
                return true;
            }
            requires.clear();
            if (filterInfoList.isEmpty()) {
                haveValidated.compareAndSet(true, false);
                throw new DRCClientException("Filter list is empty, use addFilterTuple add filter tuple");
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (FilterInfo filterInfo : filterInfoList) {
                String dbName = filterInfo.getDbName();
                String tableName = filterInfo.getTableName();
                String tenant = filterInfo.getTenant();
                List<String> cols = filterInfo.getColsList();
                String formatDbName = null;
                if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
                    throw new DRCClientException("DBName and TableName is strictly required, Current filter tuple: "
                            + tenant + "," + dbName + "," + tableName + ",[" + StringUtils.join(cols, ".") + "]");
                }
                if (dbType == DBType.OCEANBASE1) {
                    if (StringUtils.isEmpty(tenant)) {
                        haveValidated.compareAndSet(true, false);
                        throw new DRCClientException("Target database is OB1.0, tenant is strictly required, Current filter tuple: "
                                + tenant + "," + dbName + "," + tableName + ",[" + StringUtils.join(cols, ".") + "]");
                    }
                    formatDbName = tenant + FILTER_SEPARATOR_INNER + dbName;
                } else {
                    formatDbName = dbName;
                }
                stringBuilder.append(formatDbName).append(FILTER_SEPARATOR_INNER).append(tableName).append(FILTER_SEPARATOR);
                if (null == cols || 0 == cols.size()) {
                    haveValidated.compareAndSet(true, false);
                    throw new DRCClientException("Col filter must be set, Current filter tuple: "
                            + tenant + "," + dbName + "," + tableName + ",[" + StringUtils.join(cols, ".") + "]");
                }
                for (String col : cols) {
                    //here, we don't use trim in case that  " *" or "* " or " * " is kind of col names
                    if (!"*".equals(col)) {
                        isAllMatch = false;
                    }
                }
                DataFilterUtil.putColNames(formatDbName, tableName, cols, this);
            }
            storeFilter = stringBuilder.toString();
            return true;
        }
    }

    /**
     * Set branch DB interface is designed for compatible to the user who only pass table.cols in filter string.
     *
     * @param branchDb
     */
    @Deprecated
    @Override
    public void setBranchDb(String branchDb) {
        this.branchDB = branchDb;
    }

    public String toString() {
        return storeFilter;
    }

    @Override
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

    @Override
    public Map<String, Map<String, List<String>>> dstoreRequiredMap() {
        parseFilterInfoList();
        return getRequireMap();
    }

    private void parseFilterInfoList() throws DRCClientException {
        if (!hasParsedFilter) {
            logger.info("parse the filter info list");

            StringBuilder stringBuilder = new StringBuilder();

            for (FilterInfo filterInfo : filterInfoList) {
                String dbName = filterInfo.getDbName();
                String tableName = filterInfo.getTableName();
                String tenant = filterInfo.getTenant();
                List<String> cols = filterInfo.getColsList();
                String formatDbName = null;
                if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
                    throw new DRCClientException("DBName and TableName is strictly required, Current filter tuple: "
                            + tenant + "," + dbName + "," + tableName + ",[" + StringUtils.join(cols, ".") + "]");
                }

                formatDbName = dbName;

                stringBuilder.append(formatDbName).append(FILTER_SEPARATOR_INNER).append(tableName).append(FILTER_SEPARATOR);
                if (null == cols || 0 == cols.size()) {
                    haveValidated.compareAndSet(true, false);
                    throw new DRCClientException("Col filter must be set, Current filter tuple: "
                            + tenant + "," + dbName + "," + tableName + ",[" + StringUtils.join(cols, ".") + "]");
                }
                for (String col : cols) {
                    //here, we don't use trim in case that  " *" or "* " or " * " is kind of col names
                    if (!"*".equals(col)) {
                        isAllMatch = false;
                    }
                }
                logger.info("formatDbName: " + formatDbName + ", tableName: " + tableName + ", cols: " + cols);

                DataFilterUtil.putColNames(formatDbName, tableName, cols, this);
            }
            storeFilter = stringBuilder.toString();

            hasParsedFilter = true;
        }

    }
}
