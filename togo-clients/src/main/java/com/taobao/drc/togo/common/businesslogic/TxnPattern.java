package com.taobao.drc.togo.common.businesslogic;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by longxuan on 18/1/25.
 */
public class TxnPattern {
    public static class DBAndTableRegexPair {
        public final String dbNameRegex;
        public final String tableNameRegex;
        public DBAndTableRegexPair(String dbNameRegex, String tableNameRegex) {
            this.dbNameRegex = dbNameRegex;
            this.tableNameRegex = tableNameRegex;
        }
    }
    public static final String TABLE_SPLITTER = "\\|";
    public static final String TABLE_APPENDER = "|";
    public static final String DB_SPLITTER = ";";
    public static final String DB_APPENDER = DB_SPLITTER;
    private List<DBAndTableRegexPair> pairs;
    private void checkContainsCharacter(String value, String toCheck) {
        if (value.contains(toCheck)) {
            throw new RuntimeException("TxnPatternHelper: " + value + " contains " + toCheck);
        }
    }

    public TxnPattern() {
        pairs = new LinkedList<>();
    }

    public TxnPattern addDBAndTableRegex(String dbRegex, String tableRegex) {
        checkContainsCharacter(dbRegex + tableRegex, DB_APPENDER);
        checkContainsCharacter(dbRegex + tableRegex, TABLE_APPENDER);
        pairs.add(new DBAndTableRegexPair(dbRegex, tableRegex));
        return this;
    }

    public String buildTxnPattern() {
        if (pairs.isEmpty()) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder();
        pairs.forEach((dbAndTableRegexPair -> {
            stringBuilder.append(dbAndTableRegexPair.dbNameRegex)
                    .append(TABLE_APPENDER)
                    .append(dbAndTableRegexPair.tableNameRegex)
                    .append(DB_APPENDER);
        }));
        return stringBuilder.toString();
    }

    private TxnPattern(List<DBAndTableRegexPair> txnPattern) {
        pairs = txnPattern;
    }

    public List<DBAndTableRegexPair> getPairs() {
        return pairs;
    }

    public static TxnPattern fromString(String txnPattern) {
        if (null == txnPattern) {
            return null;
        }
        List<DBAndTableRegexPair> pairs = new LinkedList<>();
        String[] dbAndTablePairs = txnPattern.split(DB_SPLITTER);
        for (String dbAndTableRegexPair : dbAndTablePairs) {
            String[] dbAndTableRegexArray = dbAndTableRegexPair.split(TABLE_SPLITTER);
            if (dbAndTableRegexArray.length != 2) {
                throw new RuntimeException("dbAndTableRegexArray length required 2, the origin value is " + dbAndTableRegexPair);
            }
            pairs.add(new DBAndTableRegexPair(dbAndTableRegexArray[0], dbAndTableRegexArray[1]));
        }
        return new TxnPattern(pairs);
    }

}
