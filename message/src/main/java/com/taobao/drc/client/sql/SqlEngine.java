package com.taobao.drc.client.sql;

import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.message.ByteString;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.message.DataMessage.Record.Field;
import com.taobao.drc.client.message.DataMessage.Record.Field.Type;
import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

class InnerTable {
	// public String db_table;
	public String db_name;
	public String table_name;
	public String select_str;
	public String create_table;
	public Table table;
	public Select select;
	public Expression condition;
	public TableFilter filter;
	public Row row;

	public InnerTable(String db, String table, String select) {
		this.db_name = db;
		this.table_name = table;
		this.select_str = select;
	}
}

public class SqlEngine {
	private boolean storeRecords;
	private Database database;
	private Session session;
	private Parser parser;
	private HashMap<String, HashMap<String, InnerTable>> inner_tables;
	private long end;
	private boolean exit;
	private StringBuilder resultBuilder;
	long startMillisecond;

	public SqlEngine(boolean storeRecords) {
		this.storeRecords = storeRecords;
		database = null;
		session = null;
		parser = null;
		inner_tables = null;
		resultBuilder = new StringBuilder();
		startMillisecond = System.currentTimeMillis();
	}

	public void init() {
		inner_tables = new HashMap<String, HashMap<String, InnerTable>>();
		ConnectionInfo conn = new ConnectionInfo(".");
		database = new Database(conn, "");
		User admin_user = new User(database, 0, "admin", true);
		admin_user.setAdmin(true);
		session = new Session(database, admin_user, 0);
		parser = new Parser(session);
		// create user by SQL
		String createUser = "create user myuser password 'mypassword' admin";
		parser.prepare(createUser).update();
	}

	static void ShowSchemas(Database database) {
		for (Schema schema : database.getAllSchemas()) {
			System.out.println(schema.toString());
			for (Table table : schema.getAllTablesAndViews()) {
				System.out.println("\t" + table.getName());
			}
		}
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public boolean checkExit(Record record) throws Exception {
		if (exit)
			return true;
		else if (Long.valueOf(record.getTimestamp()) > end) {
			System.out.println("sql engine exit normally");
			saveResults();
			synchronized (this) {
				notify();
			}
			exit = true;

		}
		return false;
	}

	public void exitError(String errorMessage) {
		System.out.println("sql engine exit with error");
		resultBuilder.append(errorMessage).append("\n");
		synchronized (this) {
			notify();
		}
		exit = true;
	}

	public void addFilter(String select) throws Exception {
		String select2 = select.toLowerCase();
		select2 = select2.substring(select2.indexOf("from") + 4);
		select2 = select2.trim();
		int endIndex = select2.indexOf(' ');
		if (endIndex >= 0)
			select2 = select2.substring(0, select2.indexOf(' '));
		String names[] = select2.split("\\.");
		String db = names[0];
		String table = names[1];
		addFilter(db, table, select);
	}

	public void addFilter(String db, String table, String select) throws Exception {
		HashMap<String, InnerTable> db_tables = inner_tables.get(db);
		try {
			if (db_tables == null) {
				// TODO create InnerTable
				String create_db = "create schema " + db + " AUTHORIZATION myuser";
				parser.prepare(create_db).update();
				// ShowSchemas(database);
				db_tables = new HashMap<String, InnerTable>();
				inner_tables.put(db, db_tables);
			}
		} catch (Exception e) {
			System.out.println(e.toString());
			throw e;
		}
		System.out.println("create database " + db);
		db_tables.put(table, new InnerTable(db, table, select));
	}

	public static int getIncrease(Record record) {
		int increase = 1;
		switch (record.getOpt()) {
		case DELETE:
			break;
		case INSERT:
			if (record.getDbType() != DBType.OCEANBASE)
				break;
		case UPDATE:
		case REPLACE:
			increase = 2;
			break;
		default:
			increase = 0;
		}
		return increase;
	}

	// return true if accepted
	public boolean createTableOnFirstRecord(Record record, InnerTable t, int increase) throws Exception {
		if (increase == 0) {
			return false;
		}
		if (t == null) {
			return false;
		}
		if (t.create_table != null)
			return true;

		List<Field> field_list = record.getFieldList();
		// extended columns: drc_id, drc_timestamp, drc_type
		String create_table = "CREATE TABLE " + t.db_name + "." + t.table_name
				+ " (drc_id INT PRIMARY KEY AUTO_INCREMENT, drc_timestamp long, drc_type VARCHAR(10), physical_table VARCHAR(64)";
		for (int i = increase - 1; i < field_list.size(); i += increase) {
			Field field = field_list.get(i);
			String type_str = getH2TypeName(field.getType());
			create_table += ", " + field.getFieldname() + " " + type_str;
		}
		create_table += ")";
		System.out.println(create_table);
		t.create_table = create_table;
		parser.prepare(create_table).update();
		t.table = database.getSchema(t.db_name.toUpperCase()).getTableOrViewByName(t.table_name.toUpperCase());
		t.select = (Select) parser.prepare(t.select_str);
		t.filter = t.select.getTopTableFilter();
		t.condition = t.select.getCondition();
		t.row = t.table.getTemplateRow();
		return true;
	}

	public boolean isEmpty() {
		return inner_tables.isEmpty();
	}

	public InnerTable getInnerTable(Record record) {
		String db = getLogicalName(record.getDbname());
		String table = getLogicalName(record.getTablename());
		HashMap<String, InnerTable> db_tables = inner_tables.get(db);
		if (db_tables == null)
			return null;
		return db_tables.get(table);
	}

	// TODO it is not OK to just add '*' after a logical DB/table name
	public String getFnmatchString() {
		String result = "";
		for (Entry<String, HashMap<String, InnerTable>> db_entry : inner_tables.entrySet()) {
			String db = db_entry.getKey();
			for (String table : db_entry.getValue().keySet()) {
				if (!result.isEmpty())
					result += "|";
				result += db + "*;" + table + "*;*";
			}
		}
		if (result.isEmpty())
			result = "*;*;*";
		System.out.println("fnmatch string: " + result);
		return result;
	}

	private String getLogicalName(String name) {
		if (name.matches(".*_[0-9]*")) {
			return name.substring(0, name.lastIndexOf('_'));
		}
		return name;
	}

	public boolean filterByCondition(Record record, InnerTable t, int increase) {
		// TODO set values
		Row tmp_row = t.table.getTemplateRow();
		recordToRow(record, tmp_row, increase);
		if (t.filter != null)
			t.filter.set(tmp_row);
		if (t.condition == null || t.condition.getBooleanValue(session).booleanValue()) {
			if (storeRecords) {
				t.table.addRow(session, tmp_row);
			}
			return true;
		}
		return false;
	}

	private String getH2TypeName(Type type) {
		switch (type) {
		case INT8:
			return "TYNIINT";
		case INT16:
			return "SMALLINT";
		case INT24:
			return "INT";
		case INT32:
			return "INT";
		case INT64:
			return "BIGINT";
		case DECIMAL:
			return "DECIMAL";
		case FLOAT:
			return "DOUBLE";
		case DOUBLE:
			return "DOUBLE";
		case TIMESTAMP:
			return "TIMESTAMP";
		case DATE:
			return "DATE";
		case TIME:
		case DATETIME:
			return "TIME";
		case STRING:
			return "VARCHAR(1024)";
		case NULL:
		case YEAR:
		case BIT:
		case ENUM:
		case SET:
		case BLOB:
		case GEOMETRY:
        case UNKOWN:
		default:
			throw DbException.getUnsupportedException(type.toString());
		}
	}

	private Value getValue(Field field) {
		ByteString value = field.getValue();
		if (value == null)
			return ValueNull.INSTANCE;
		Type type = field.getType();
		switch (type) {
		case INT8:
			return ValueByte.get(value.getBytes()[0]);
		case INT16:
			return ValueShort.get(Short.valueOf(value.toString()));
		case INT24:
			return ValueInt.get(Integer.valueOf(value.toString()));
		case INT32:
			return ValueInt.get(Integer.valueOf(value.toString()));
		case INT64:
			return ValueLong.get(Long.valueOf(value.toString()));
		case DECIMAL:
			return ValueDecimal.get(new BigDecimal(value.toString()));
		case FLOAT:
			return ValueFloat.get(Float.valueOf(value.toString()));
		case DOUBLE:
			return ValueDouble.get(Double.valueOf(value.toString()));
		case NULL:
			return ValueNull.INSTANCE;
		case TIMESTAMP:
			return ValueTimestamp.get(Timestamp.valueOf(value.toString()));
		case DATE:
			// TODO
			return ValueDate.get(Date.valueOf(value.toString().replaceAll("[^0-9]", "-")));
		case TIME:
		case DATETIME:
			// TODO
			return ValueTime.get(Time.valueOf(value.toString()));
		case STRING:
			return ValueString.get(value.toString());
		case YEAR:
		case BIT:
		case ENUM:
		case SET:
		case BLOB:
		case GEOMETRY:
        case UNKOWN:
		default:
			throw DbException.getUnsupportedException(type.toString());
		}
	}

	private void recordToRow(Record record, Row row, int increase) {
		// set record id
		row.setValue(0, ValueLong.get(Long.valueOf(record.getId())));
		// set record timestamp
		row.setValue(1, ValueLong.get(Long.valueOf(record.getTimestamp())));
		// set record type
		row.setValue(2, ValueString.get(record.getOpt().toString()));
		// set record table
		row.setValue(3, ValueString.get(record.getDbname() + "." + record.getTablename()));
		// set new values of columns
		List<Field> field_list = record.getFieldList();
		Field field;
		for (int i = increase - 1; i < field_list.size(); i += increase) {
			field = field_list.get(i);
			// System.out.println("setValue("+3+i/increase+",
			// "+getValue(field).toString()+")");
			row.setValue(4 + i / increase, getValue(field));
		}
	}

	public LocalResult query(String db, String table) {
		HashMap<String, InnerTable> tables = inner_tables.get(db);
		if (tables == null)
			return null;
		InnerTable inner_table = tables.get(table);
		if (inner_table == null)
			return null;
		// TODO
		LocalResult result = inner_table.select.query(100);
		return result;
	}

	public static void ShowResult(LocalResult result) {
		if (result == null) {
			System.out.println("empty result");
			return;
		}
		while (result.next()) {
			Value[] row = result.currentRow();
			for (int j = 0; j < row.length; j++) {
				System.out.print(row[j] + " ");
			}
			System.out.println();
		}
		System.out.println(result);
		System.out.println("row count = " + result.getRowCount());
	}

	public void waitExit() throws InterruptedException {
		System.out.println("waiting exit");
		synchronized (this) {
			wait();
		}
		System.out.println("finish waiting");
	}

	private String saveResult(LocalResult result) {
		if (result == null) {
			resultBuilder.append("empty result\n");
			return resultBuilder.toString();
		}
		resultBuilder.append(": ").append(result.getRowCount()).append(" rows\n");
		while (result.next()) {
			Value[] row = result.currentRow();
			for (int j = 0; j < row.length; j++) {
				resultBuilder.append(row[j]).append(" ");
			}
			resultBuilder.append("\n");
		}
		return resultBuilder.toString();
	}

	private void saveResults() {
		for (Entry<String, HashMap<String, InnerTable>> db_entry : inner_tables.entrySet()) {
			String db = db_entry.getKey();
			for (String table : db_entry.getValue().keySet()) {
				resultBuilder.append(db + "." + table);
				LocalResult result = query(db, table);
				saveResult(result);
			}
		}
		double queryTime = (double) (System.currentTimeMillis() - startMillisecond) / 1000;
		resultBuilder.append(queryTime).append(" sec\n");
	}

	public String getResult() {
		return resultBuilder.toString();
	}
}
