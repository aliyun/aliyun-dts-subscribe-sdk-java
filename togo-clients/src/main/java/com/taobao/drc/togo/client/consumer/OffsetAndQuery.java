package com.taobao.drc.togo.client.consumer;

/**
 * @author yangyang
 * @since 17/3/8
 */
public class OffsetAndQuery {
    private final QueryCondition condition;
    private final long offset;

    public OffsetAndQuery(QueryCondition condition, long offset) {
        this.condition = condition;
        this.offset = offset;
    }

    public QueryCondition getCondition() {
        return condition;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "OffsetAndQuery{" +
                "condition=" + condition +
                ", offset=" + offset +
                '}';
    }
}
