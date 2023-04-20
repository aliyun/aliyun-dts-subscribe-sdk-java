package org.apache.kafka.common.record.meta;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.List;

public abstract class RecordMeta {
    private Errors error = Errors.NONE;

    public abstract String getName();

    public abstract Struct toStruct(String keyName, Struct parent);

    public Errors getError() {
        return error;
    }

    public void setError(Errors error) {
        this.error = error;
    }

    protected static List<String> toStringList(Object[] objects) {
        List<String> ret = null;
        if (null != objects) {
            ret = new ArrayList<>(objects.length);
            for (Object object : objects) {
                String item = (String) object;
                ret.add(item);
            }
        }
        return ret;
    }
}
