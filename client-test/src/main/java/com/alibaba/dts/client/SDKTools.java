package com.alibaba.dts.client;

import com.taobao.drc.client.DRCClient;
import com.taobao.drc.client.DataFilter;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.impl.DRCClientImpl;
import com.taobao.drc.client.message.DataMessage;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * the simple sdk client to test.
 * @author xusheng.zkw
 */
public class SDKTools {
    private static String FILTER_KEY = "tools.filter";
    private static String BLACK_FILTER_KEY = "tools.blackFilter";
    private static String LOG_KEY = "tools.log";
    private static String USER_KEY = "tools.user";
    private static String PASSWORD_KEY = "tools.password";
    private static String SUBSCRIBE_KEY = "tools.subscribe";
    private static String TIMESTAMP_KEY = "tools.timestamp";
    private static String MULTI_KEY = "tools.multi";
    private static String REQUIRE_TX_KEY = "tools.require.tx";
    private static String FILTER_EMPTY_TX_KEY = "tools.filter.emptyTX";
    private static String SAVEDPOINT_KEY = "tools.out.savedpoint";
    private static String ASK_SELFUNIT_USETHREADID_KEY = "tools.askSelfUnitUseThreadID";
    private static String ASK_OTHERUNIT_USETHREADID_KEY = "tools.askOtherUnitUseThreadID";
    private static String PRINT_RECORD_KEY = "tools.printRecord";
    private static String LAZY_DECODE_RECORD_KEY = "tools.decode.lazy";
    private static String STATISTIC_INTERVAL_KEY = "tools.statistic.interval";
    private static String STATISTIC_LOG_KEY = "tools.statistic.log";
    private static String DTS_MARK_KEY = "tools.dtsMark";
    private static String DRC_MARK_KEY = "tools.drcMark";

    private static String ASK_SELFUNIT_KEY = "tools.askSelfUnit";
    private static String ASK_OTHERUNIT_KEY = "tools.askOtherUnit";

    /**
     * not only tools.xxx but also manager.host is required.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("You are running SDKTools for test. The only arg is a Properties file path which filled params that subscription needed.");
        printConfigNeed();
        if (args.length < 1) {
            throw new RuntimeException("Properties config required.");
        }
        Properties properties = loadFromFile(args[0]);
        if (Boolean.valueOf(properties.getProperty(LOG_KEY, "true"))) {
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
            System.setProperty("org.apache.commons.logging.simplelog.defaultlog", "info");
        }
        System.out.println(properties);
        System.out.println("\n\r");
        DRCClient client = new DRCClientImpl(properties);
        DataFilter filter = new DataFilter(properties.getProperty(FILTER_KEY));
        client.addDataFilter(filter);
        if (StringUtils.isNotEmpty(properties.getProperty(BLACK_FILTER_KEY))) {
            client.setBlackList(StringUtils.trimToEmpty(properties.getProperty(BLACK_FILTER_KEY)));
        }
        if (StringUtils.isNotEmpty(properties.getProperty(DTS_MARK_KEY))) {
            client.setDtsMark(properties.getProperty(DTS_MARK_KEY));
            if (!StringUtils.trimToEmpty(properties.getProperty(DTS_MARK_KEY)).startsWith("!")) {
                System.out.println("Ask self unit, dts mark: " + properties.getProperty(DTS_MARK_KEY));
                client.askSelfUnit();
            }
        }
        if (StringUtils.isNotEmpty(properties.getProperty(DRC_MARK_KEY))) {
            client.setDrcMark(properties.getProperty(DRC_MARK_KEY));
            if (!StringUtils.trimToEmpty(properties.getProperty(DRC_MARK_KEY)).startsWith("!")) {
                System.out.println("Ask self unit, drc mark: " + properties.getProperty(DTS_MARK_KEY));
                client.askSelfUnit();
            }
        }
        client.filterEmptyTxnMark(Boolean.valueOf(properties.getProperty(FILTER_EMPTY_TX_KEY, "true")));

        final ConsumeStatistics statistics = new ConsumeStatistics(Integer.parseInt(properties.getProperty(STATISTIC_INTERVAL_KEY, "1")), properties.getProperty(STATISTIC_LOG_KEY, "./statistics.log"));
        final boolean printRecord = Boolean.valueOf(properties.getProperty(PRINT_RECORD_KEY, "true"));
        final boolean lazyDecode = !printRecord && Boolean.valueOf(properties.getProperty(LAZY_DECODE_RECORD_KEY, "true"));
        client.addListener(new Listener() {
            @Override
            public void notify(DataMessage message) {
                statistics.getFirstRecord();
                for (DataMessage.Record r : message.getRecordList()) {
                    statistics.hit();
                    String recordValue = lazyDecode ? "" : r.toString();
                    statistics.setRecordTime(r.getTimestamp());
                    if (printRecord) {
                        System.out.println(StringUtils.isEmpty(recordValue) ? r.toString() : recordValue);
                        System.out.println("------------------------");
                    }
                    /**
                     * ?topic???????????????????????????????????????????
                     *
                     */
                    if (r.getCheckpointManager().isMultiMode()) {
                        r.ackAsConsumed();
                    }
                }
            }

            @Override
            public void notifyRuntimeLog(String level, String log) {
                System.out.println("[" + level + "]" + log);
            }

            @Override
            public void handleException(Exception e) {
                System.out.println(e.getMessage());
            }
        });
        boolean requireTx = Boolean.valueOf(properties.getProperty(REQUIRE_TX_KEY, Boolean.TRUE.toString()));
        client.requireTxnMark(requireTx);
        if (Boolean.valueOf(properties.getProperty(ASK_SELFUNIT_USETHREADID_KEY, Boolean.FALSE.toString()))) {
            client.askSelfUnitUseThreadID();
        }
        if (Boolean.valueOf(properties.getProperty(ASK_OTHERUNIT_USETHREADID_KEY, Boolean.FALSE.toString()))) {
            client.askOtherUnitUseThreadID();
        }

        if (Boolean.valueOf(properties.getProperty(ASK_SELFUNIT_KEY, Boolean.FALSE.toString()))) {
            System.out.println("Ask self unit");
            client.askSelfUnit();
        }

        statistics.start();
        client.initService(properties.getProperty(USER_KEY), properties.getProperty(SUBSCRIBE_KEY), properties.getProperty(PASSWORD_KEY), properties.getProperty(TIMESTAMP_KEY), properties.getProperty(SAVEDPOINT_KEY, "./safepoint.log"));
        if (Boolean.valueOf(properties.getProperty(MULTI_KEY, Boolean.FALSE.toString()))) {
            client.startMultiService().join();
        } else {
            client.startService().join();
        }
    }

    public static Properties loadFromFile(String logFile) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(logFile));
        return properties;
    }

    private static void printConfigNeed() {
        StringBuffer sb = new StringBuffer();
        sb.append("DESCRIPTION").append("\n\r");
        sb.append("     Config file need at least contains:").append("\n\r");
        sb.append("     tools.filter=*.*.*        data filter").append("\n\r");
        sb.append("     tools.blackFilter=db1.table1|db2.table2         black data filter").append("\n\r");
        sb.append("     tools.log=true/false     log4j log level").append("\n\r");
        sb.append("     tools.user=              RM user account").append("\n\r");
        sb.append("     tools.password=          RM user password").append("\n\r");
        sb.append("     tools.subscribe=         dbname/appname/topic/subtopic you want subscribe").append("\n\r");
        sb.append("     tools.timestamp=         unix timestamp that data generation time").append("\n\r");
        sb.append("     tools.multi=true/false   multi-topic subscription").append("\n\r");
        sb.append("     tools.out.savedpoint=    consumed safepoint output path").append("\n\r");
        sb.append("     manager.host=            CM address").append("\n\r");
        sb.append("EXAMPLES").append("\n\r");
        sb.append("     java -jar xxx.jar configFilePath");
        sb.append("\n\r");
        System.out.println(sb.toString());
    }

    private static class ConsumeStatistics extends Thread {
        private AtomicLong hits = new AtomicLong();
        private volatile Long firstGetRecordTime = null;
        private AtomicBoolean isGetFirstRecord = new AtomicBoolean(false);
        private final int interval;
        private final FileWriter log;
        private volatile String recordTime;

        public ConsumeStatistics(int interval, String log) throws IOException {
            this.interval = interval;
            this.log = new FileWriter(new File(log), true);
        }

        public void getFirstRecord() {
            if (!isGetFirstRecord.get() && isGetFirstRecord.compareAndSet(false, true)) {
                firstGetRecordTime = System.currentTimeMillis();
            }
        }

        public void hit() {
            hits.incrementAndGet();
        }

        public void printStatisticData() throws IOException {
            long currentHits = hits.get();
            long currentTime = System.currentTimeMillis();
            long span = null != firstGetRecordTime ? (currentTime - firstGetRecordTime) / 1000 : 0;
            long qps = span > 0 ? currentHits / span  : 0;
            StringBuilder printInfo = new StringBuilder();
            printInfo.append("total:").append(currentHits).append("\t");
            printInfo.append("span:").append(span).append("\t");
            printInfo.append("rps:").append(qps).append("\t");
            if (null != recordTime) {
                printInfo.append("recordTimestamp:").append(new Date(Long.parseLong(recordTime) * 1000).toLocaleString()).append("\t");
                printInfo.append("diff:").append((currentTime / 1000 - Long.parseLong(recordTime))).append("\t");
            } else {
                printInfo.append("recordTimestamp:-").append("\t");
                printInfo.append("diff:-").append("\t");
            }
            printInfo.append("\r\n");
            log.write(printInfo.toString());
            log.flush();
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                try {
                    sleep(interval * 1000);
                    printStatisticData();
                } catch (Throwable e) {
                    break;
                }
            }
        }

        public String getRecordTime() {
            return recordTime;
        }

        public void setRecordTime(String recordTime) {
            this.recordTime = recordTime;
        }
    }
}
