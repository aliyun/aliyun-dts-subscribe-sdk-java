package com.aliyun.dts.subscribe.clients.filter;

import com.aliyun.dts.subscribe.clients.exception.CriticalException;
import com.aliyun.dts.subscribe.clients.filter.utils.DataFilterUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataFilterImpl implements DataFilter {
    private static final Logger logger = LoggerFactory.getLogger(DataFilterImpl.class);

    /**
     * Class store the raw info user passed.
     */
    public class FilterInfo {
        private String tenant;
        private String dbName;
        private String tableName;
        private List<String> colsList;

        public FilterInfo(String tenant, String dbName, String tableName, String... cols) {
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

    private List<FilterInfo> filterInfoList;
    private boolean isAllMatch;
    private String storeFilter;
    private AtomicBoolean haveValidated;
    // Still, we keep logical insist with old data filter  impl, use 2 map divide add op and find op.
    private Map<String, Map<String, List<String>>> requires;
    private Map<String, Map<String, List<String>>> dbTableColsReflectionMap;

    //parse the filterInfoList to requires
    private boolean hasParsedFilter = false;

    private String blackList;

    private DataFilterImpl(List<FilterInfo> filterInfos) {
        if (null == filterInfos) {
            filterInfoList = new LinkedList<FilterInfo>();
        } else {
            filterInfoList = filterInfos;
        }
        haveValidated = new AtomicBoolean(false);
        requires = new HashMap<String, Map<String, List<String>>>();
        dbTableColsReflectionMap = new HashMap<String, Map<String, List<String>>>();
        isAllMatch = true;
        storeFilter = null;
    }

    private DataFilterImpl() {
        this(null);
    }

    public static DataFilterImpl create() {
        return new DataFilterImpl();
    }

//    public static DataFilterBaseImpl create(DBType dbType) {
//    }

    public DataFilterImpl addFilterTuple(String tenant, String db, String table, String... cols) {
        FilterInfo filterInfo = new FilterInfo(tenant, db, table, cols);
        this.filterInfoList.add(filterInfo);
        return this;
    }


    public List<FilterInfo> getFilterInfoList() {
        return this.filterInfoList;
    }

    @Override
    public String getConnectStoreFilterConditions() {
        return storeFilter;
    }

    @Override
    public boolean validateFilter() {
        synchronized (this) {
            if (!haveValidated.compareAndSet(false, true)) {
                return true;
            }
            requires.clear();
            if (filterInfoList.isEmpty()) {
                haveValidated.compareAndSet(true, false);
                throw new CriticalException("Filter list is empty, use addFilterTuple add filter tuple");
            }
            StringBuilder stringBuilder = new StringBuilder();
            for (FilterInfo filterInfo : filterInfoList) {
                String dbName = filterInfo.getDbName();
                String tableName = filterInfo.getTableName();
                String tenant = filterInfo.getTenant();
                List<String> cols = filterInfo.getColsList();
                String formatDbName = null;
                if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tableName)) {
                    throw new CriticalException("DBName and TableName is strictly required, Current filter tuple: "
                            + tenant + "," + dbName + "," + tableName + ",[" + StringUtils.join(cols, ".") + "]");
                }
                formatDbName = dbName;
                stringBuilder.append(formatDbName).append(FILTER_SEPARATOR_INNER).append(tableName).append(FILTER_SEPARATOR);
                if (null == cols || 0 == cols.size()) {
                    haveValidated.compareAndSet(true, false);
                    throw new CriticalException("Col filter must be set, Current filter tuple: "
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

    public String toString() {
        return storeFilter;
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

    @Override
    public boolean getIsAllMatch() {
        return isAllMatch;
    }

    private void parseFilterInfoList() throws CriticalException {
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
                    throw new CriticalException("DBName and TableName is strictly required, Current filter tuple: "
                            + tenant + "," + dbName + "," + tableName + ",[" + StringUtils.join(cols, ".") + "]");
                }

                formatDbName = dbName;

                stringBuilder.append(formatDbName).append(FILTER_SEPARATOR_INNER).append(tableName).append(FILTER_SEPARATOR);
                if (null == cols || 0 == cols.size()) {
                    haveValidated.compareAndSet(true, false);
                    throw new CriticalException("Col filter must be set, Current filter tuple: "
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

    public String getBlackList() {
        return blackList;
    }

    public void setBlackList(String blackList) {
        this.blackList = blackList;
    }
}
