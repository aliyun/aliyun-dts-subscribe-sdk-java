package com.taobao.drc.client.sql;

import com.taobao.drc.client.message.DataMessage.Record;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class SqlHandler extends ChannelInboundHandlerAdapter {
	private SqlEngine engine;

	public SqlEngine getEngine() {
		return engine;
	}

	public void setEngine(SqlEngine engine) {
		this.engine = engine;
	}

	public SqlHandler(SqlEngine engine) {
		if (engine == null) {
			this.engine = new SqlEngine(false);
			this.engine.init();
		} else {
			this.engine = engine;
		}
	}

	void PrintRecord(Record record) {
		System.out.printf("%s %s.%s %s\n", record.getTimestamp(), record.getDbname(), record.getTablename(), record.getOpt().toString());
	}

	/*
	 * 1) get database and table name, and transform them to one logical name 2)
	 * if the name is not included by SQL-select, filter them 3) if the name
	 * show up for the first time, create table on the internal SQL engine, and
	 * create where-condition 4) get value of where-condition, if false, filter
	 * them
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Record record = (Record) msg;
//		 PrintRecord(record);
		try {
			if (engine.checkExit(record)) {
//				System.out.println("filtered by exit");
				return;
			}
			if (!engine.isEmpty()) {

				int increase = SqlEngine.getIncrease(record);
				// System.out.println("increase=" + increase);
				SqlEngine.InnerTable t = engine.getInnerTable(record);
				if (!engine.createTableOnFirstRecord(record, t, increase)) {
//					 System.out.println("filtered by table");
					return;
				}
				if (!engine.filterByCondition(record, t, increase)) {
//					 System.out.println("filtered by sql");
					return;
				}
			}
		} catch (Exception e) {
			engine.exitError(e.toString());
			e.printStackTrace();
			throw e;
		}
		// System.out.println("pass");
		ctx.fireChannelRead(msg);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();
	}

}
