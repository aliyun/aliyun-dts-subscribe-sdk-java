package com.taobao.drc.client.network;

import com.taobao.drc.client.DataFilterBase;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.message.DataMessage.Record.Field;
import com.taobao.drc.client.utils.DataFilterUtil;

import java.util.Iterator;
import java.util.List;

/**
 * Created by jianjundeng on 10/2/14.
 */
public class RecordPreNotifyHelper {
    private UserConfig userConfig;
    public RecordPreNotifyHelper(UserConfig config) {
        this.userConfig = config;
    }

    public void process(Record record) {
        //add region info
        record.setRegionId(userConfig.getRegionInfo());
        //column filter
        processColumnFilter(userConfig, record);
    }

    private void processColumnFilter(UserConfig userConfig, Record record) {
        DataFilterBase filter = userConfig.getDataFilter();
        if (!filter.getIsAllMatch()) {
            List<String> s = DataFilterUtil.getColNamesWithMapping(record.getDbname(), record.getTablename(), filter);
            if (s != null) {
                List<Field> l = record.getFieldList();
                Iterator<Field> it = l.iterator();
                while (it.hasNext()) {
                    Field f = it.next();
                    if (!DataFilterUtil.isColInArray(f.getFieldname(), s)) {
                        it.remove();
                    }
                }
            }
        }
    }
}
