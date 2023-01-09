package com.taobao.drc.client;


import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.sql.SqlEngine;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.util.Map;

/**
 * Client APIs for DRC service.
 */
public interface DRCClient {

    /*
     * below methods use to initiate DRCClient.
     */

    /**
     * Used by users to initialize the service by gmtModified.
     *
     * @param groupName is the name of the users' group.
     * @param dbName is the partial database name.
     * @param identification is the user's password.
     * @param startingPoint is either "checkpoint" or "gmt_modified".
     * @param localFilename path to binary log file.
     *
     * @throws IOException log file related IO errors.
     */
    void initService(final String groupName,
                     final String dbName,
                     final String identification,
                     final String startingPoint,
                     final String localFilename)
            throws Exception;

    /**
     * Used by users to reconnect according to the log fle.
     * @param groupName groupName is the name of the users' group.
     * @param dbName dbName is the partial database name.
     * @param identification is the user's password.
     * @param localFilename is path to binary log file.
     * @throws Exception if the log file is not readable.
     */
    void initService(final String groupName,
                     final String dbName,
                     final String identification,
                     final String localFilename)
            throws Exception;

    /**
     * Initialize the service by manually setting restarted checkpoint.
     * Notice that the user should also provide the respective meta data version
     * and assigned task name.
     *
     * @param groupName is the name of the users' group.
     * @param dbName is the partial database name.
     * @param identification is the user's password.
     * @param startingPoint is either "checkpoint" or "gmt_modified".
     * @param metaVersion is the metadata version.
     * @param taskName is the task name.
     * @param mysql is the address of the last database instance.
     * @param localFilename  is path to binary log file.
     *
     * @throws IOException if the log file cannot be created.
     */
    void initService(final String groupName,
                     final String dbName,
                     final String identification,
                     final String startingPoint,
                     final String metaVersion,
                     final String taskName,
                     final String mysql,
                     final String localFilename)
            throws Exception;

    /**
     * Initialize the service by checkpoint in memory.
     *
     * @param groupName is the name of the users' group.
     * @param dbName is the partial database name.
     * @param identification is the user's password.
     * @param checkpoint is the starting location in memory.
     * @param localFilename is path to binary log file.
     * @throws Exception if the log file cannot be created.
     */
    void initService(final String groupName,
                     final String dbName,
                     final String identification,
                     final Checkpoint checkpoint,
                     final String localFilename) throws Exception;


    /*
     * below methods use to control subscription service.
     */

    /**
     * Start the service running in a thread, the method does not block.
     * @return The launched thread.
     * @throws Exception DRCClient internal Exception.
     */
    Thread startService() throws Exception;

    /**
     * use hash mode,multiple subTopics
     * @throws Exception
     */
    Thread startMultiService()throws Exception;

    /**
     * Stop the service thread;
     */
    void stopService() throws Exception;

    /**
     * Closed active connections and set the status as not quited.
     *
     * @throws Exception
     */
    void resetService() throws Exception;

    /**
     * suspend DRC client producer.
     */
    void suspend();

    /**
     * resume DRC client producer
     */
    void resume();


    /*
     * subscribe DRCStore/DStore message notification.
     */

    /**
     * Provide the listener for data.
     * @param listener the listener who receives the message.
     */
    void addListener(Listener listener);


    /*
     * set client params via method directly.
     */

    /**
     * Add drc client required properties.
     * @param name is the name of the property.
     * @param value is the value of the property.
     */
    void addDRCConfigure(final String name, final String value);

    /**
     * Get all configures of drc client.
     * @return all configures of drc client.
     */
    Map<String, String> getDRCConfigures();

    /**
     * Get one configure for drc client.
     * @param name is the name of the configure.
     * @return the value of the configure.
     */
    String getDRCConfigure(final String name);

    /**
     * Add user-defined parameter which is sent to the server.
     * @param name is the name of the parameter.
     * @param value is the value of the parameter.
     */
    void addUserParameter(final String name, final String value);

    /**
     * Get all parameters added by the user.
     * @return all parameters.
     */
    Map<String, String> getUserParameters();

    /**
     * Get one parameter added by the user.
     * @param name is the name of the parameter.
     * @return the value of the parameter.
     */
    String getUserParameter(final String name);

    /**
     * Get partial database name.
     * @return database name.
     */
    String getDbName();

    /**
     * Add data filter to provide required tables and columns.
     * @param filter {@link  DataFilter}
     */
    void addDataFilter(DataFilterBase filter);

    void setSqlEngine(SqlEngine engine);

    /**
     * Set the frequency of heartbeat records received, one heartbeat every seconds.
     * @param everySeconds after how many seconds, has been sent one heartbeat.
     */
    void setHeartbeatFrequency(int everySeconds);

    /**
     * Set whether requiring record type begin/commit.
     * @param required true if required, false otherwise, default is true.
     */
    void requireTxnMark(boolean required);

    /**
     * whether discard empty begin/commit couple while requiring record type of begin/commit.
     * default true
     * @param filterEmptyTxnMark
     */
    void filterEmptyTxnMark(boolean filterEmptyTxnMark);

    /**
     * Set the time period to notify the listener by the time consumed.
     * @param sec is the time period.
     */
    void setNotifyRuntimePeriodInSec(long sec);

    /**
     * Set the number of records in one batch by the server.
     * @param threshold is the number of records.
     */
    void setNumOfRecordsPerBatch(int threshold);

    /**
     * Get the type of source database, current is "mysql" or "oceanbase"
     * @return the type of the source database
     */
    DBType getDatabaseType();

    /**
     * Set the group name of the client. The default name is the username,
     * but client can set it anyway.
     * @param group
     */
    void setGroup(final String group);

    /**
     * Set the subgroup name of the client. The default name is the dbName,
     * but the client can set it anyway.
     * @param subGroup
     */
    void setSubGroup(final String subGroup);

    /**
     * set regex (drc.t*x_begin4unit_mark_[0-9]* ,get current unit data)
     * @param mark regex
     */
    void setDrcMark(String mark);

    /**
     * set regex (dts.dts_trx4unit_mark_[0-9]* ,get current unit data)
     * @param mark regex
     */
    void setDtsMark(String mark);

    /**
     * get current unit data
     */
    void askSelfUnit();

    /**
     * db.table|db.table
     * @param blackList
     */
    void setBlackList(String blackList);

    /**
     * user public ip or private ip by default
     */
    void usePublicIp();

    /**
     * upper case and lower case
     */
    void useCaseSensitive();

    /**
     * Add where conditions to filter unnecessary records.
     * @param filter {@link com.taobao.drc.client.DataFilter DataFilter}
     *
     * @deprecated because now not supported
     */
    @Deprecated
    void addWhereFilter(DataFilterBase filter);

    /**
     * Filter a record if none of its columns are  really changed.
     *
     * @deprecated because now not supported
     */
    @Deprecated
    void useStrictFilter();

    /**
     * Get the task name.
     * @return the task name.
     *
     * @deprecated because no task name generated in servers
     */
    @Deprecated
    String getTaskName();

    /**
     * Get the address of the database instance.
     * @return the address of database instance.
     *
     * @deprecated because each record has its own instance (db address)
     */
    @Deprecated
    String getInstance();

    /**
     * trim blob or other type data to small size,omit some data
     */
    void trimLongType();

    /**
     * start check crc32(binary protocol)
     */
    void useCRC32Check();

    /**
     * shut down auto retry
     */
    void shutdownAutoRetry();

    /**
     * ob0.5 index table data
     */
    void needUKRecord();

    /**
     * get all stores(partition) min timestamp
     * @return
     */
    String getMultiSafeTimestamp();

    /**
     *
     */
    void setParseThreadPrefix(String prefix);

    /**
     *
     */
    void setNotifyThreadPrefix(String prefix);

    @Deprecated
    Future writeUserCtlMessage(byte[] userCtlBytes);

    /**
     * get the origin database type that client is consuming. This interface is meaningful iff startService / startMultiService is invoked
     * or it will return null
     * @return
     */
    DBType getDBType();
    void askOtherUnitUseThreadID();
    void askSelfUnitUseThreadID();
}
