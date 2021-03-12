package com.aliyun.dts.subscribe.clients.record;

public class DatabaseInfo {

    private final String databaseType;
    private final String version;

    public DatabaseInfo(String databaseType, String version) {
        this.databaseType = databaseType;
        this.version = version;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "{\"sourceType\": \"" + databaseType + "\", \"version\": \"" + version + "\"}";
    }
}
