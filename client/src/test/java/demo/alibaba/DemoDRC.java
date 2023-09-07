package demo.alibaba;

import com.taobao.drc.client.DRCClient;
import com.taobao.drc.client.DRCClientFactory;
import com.taobao.drc.client.DataFilter;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.message.DataMessage;

import java.util.Properties;

public class DemoDRC {
    private static String checkpointOfTimestamp = "1688458399";
    private static Listener messageListener = new Listener() {
        //异步通知
        @Override
        public void notify(DataMessage message) {
            for (DataMessage.Record record : message.getRecordList()) {
                System.out.println(record.getDbname()+":"+record.getTablename()+":"+record.getOpt()+":"+record.getTimestamp());
                System.out.println("record: " + record);
                //如果是多subTopic订阅，即通过client.startMultiService().join()启动订阅客户端，在消费完message后必须执行record.ackAsConsumed()。
                //record.ackAsConsumed();
                //保存已经消费的点位
                //checkpointOfTimestamp = record.getSafeTimestamp();
            }
        }

        /**
         * 目前只有WARN
         * @param level WARN/ERROR/INFO
         * @param log
         */
        @Override
        public void notifyRuntimeLog(String level, String log) {
            //运行期日志，用户需要根据日志级别做响应处理
        }

        @Override
        public void handleException(Exception e) {
            //运行期异常，，用户需要做响应处理。
        }
    };

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("manager.host", "http://daily-c.drc.dbfree.tbsite.net:8080");
        props.put("useDrcNet", "true");
        props.put("server.messageType", "binary");

        DRCClient client = null;
        //自动重试逻辑
        try {
            client = DRCClientFactory.create(DRCClientFactory.Type.MYSQL, props);
            //过滤白名单，组成：DB1;Table1;column1  多个用或隔开，列过滤建议不要开，性能损耗比较大
            DataFilter filter = new DataFilter("*;*;*");
            client.addDataFilter(filter);
            client.addListener(messageListener);
            //过滤黑名单
            client.setBlackList("db.table|db.table");

            //根据DStore threadID请求其他单元的数据
            //client.askOtherUnitUseThreadID();

            //根据DStore threadID请求本单元的数据
            //client.askSelfUnitUseThreadID();

            //根据单元化双写事务表，只拉取本单元数据，或不拉取别的单元的数据。
            //drcmark/dtsmark以!开头表示请求非本单元数据，不以!开头表示请求本单元数据
            //client.setDrcMark("drcstore 双写事务表，例如drc.t*x_begin4unit_mark_[0-9]*");
            //client.setDtsMark("dstore双写事务表， 例如!dts.dts_trx4unit_mark_[0-9]*");

            //不推送事务begin commit消息
            //client.requireTxnMark(false);

            client.initService("71215", "ali_sh_alsc_mktcoupon_new_app", "71215", checkpointOfTimestamp, null);
            client.startService().join();
            //消费同一个appName/topic下面的多个subTopic
            //client.startMultiService().join();
        } finally {
            if (null != client) {
                try {
                    client.stopService();
                } catch (Throwable e){
                }
            }
        }
    }
}
