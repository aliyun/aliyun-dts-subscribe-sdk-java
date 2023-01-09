package com.taobao.drc.client.impl;

import static com.taobao.drc.client.utils.Constant.*;

public class Checkpoint {

    private String recordId;

    private String position;

    private String timestamp;

    private String serverId;

    /**
     * dstore kafka offset
     */
    private Long offset;

    public Checkpoint() {
        recordId = position = serverId = timestamp = null;
    }

    public Checkpoint(final String recordId, final String position,
                      final String serverId, final String timestamp) {
        this.recordId = recordId;
        this.position = position;
        this.serverId = serverId;
        this.timestamp = timestamp;
    }

    public Checkpoint(final Checkpoint cp) {
        this(cp.recordId, cp.position, cp.serverId, cp.timestamp);
    }

    public boolean equals(final String cp) {
        if (position != null && cp != null)
            return position.equals(cp);
        if (timestamp != null && cp != null)
            return timestamp.equals(cp);
        return false;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(final String recordId) {
        this.recordId = recordId;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(final String position) {
        String cp = new String(position);
        if (cp.contains("@mysql-bin.")) {
            int m = cp.indexOf("@");
            int p = cp.indexOf(".");
            String cp1 = cp.substring(0, m);
            String cp2 = cp.substring(p + 1);
            long lcp2 = Long.parseLong(cp2);
            cp = cp1 + "@" + Long.toString(lcp2);
        }
        this.position = cp;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final String timestamp) {
        this.timestamp = timestamp;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String toString() {
        String cp1 = null, cp2 = null;
        if (position != null && !position.isEmpty()) {
            int in = position.indexOf('@');
            cp1 = position.substring(in + 1);
            cp2 = position.substring(0, in);
        }

        StringBuilder builder = new StringBuilder();
        if (serverId == null || serverId.isEmpty()) {
            builder.append(DELM).append(DELM);
        } else {
            int in = serverId.indexOf('-');
            String db = serverId.substring(0, in);
            String port = serverId.substring(in + 1);
            builder.append(db).append(DELM).append(port).append(DELM);
        }

        if (cp1 != null && cp2 != null) {
            builder.append(cp1).append(DELM).append(cp2).append(DELM);
        } else {
            builder.append(DELM).append(DELM);
        }


        if (timestamp != null) {
            builder.append(timestamp).append(DELM);
        } else {
            builder.append(DELM);
        }

        if (recordId != null) {
            builder.append(recordId);
        }

        return builder.toString();
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }
}
