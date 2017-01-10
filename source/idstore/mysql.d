module idstore.mysql;
version(Have_mysql_lited) {
	private import idstore.common;
	private import std.range.interfaces;
	class MySQL : Database {
		import std.traits : ReturnType;
		import mysql;
		private MySQLClient database;
		string _dbname;

		override void createDB(string dbname) {
			auto lock = database.lockConnection();
			lock.execute("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS VARCHAR(255) PRIMARY KEY)");
		}
		override void insertIDs(in string dbname, ForwardRange!string range) {
			createDB(dbname);
			auto lock = database.lockConnection();
			auto insert = inserter(lock, dbname, "IDS");
			foreach (id; range)
				insert.row(id);
			insert.flush();
		}
		override ForwardRange!string listIDs(in string dbname) {
			import std.range : inputRangeObject;
			auto lock = database.lockConnection();
			string[] output;
			lock.execute(`SELECT IDS FROM '`~_dbname~`'`, (MySQLRow row) {
				output ~= row.IDS.get!string;
			});
			return inputRangeObject(output);
		}
		override ForwardRange!string listDBs() {
			import std.range : inputRangeObject;
			string[] output;
			auto lock = database.lockConnection();
			lock.execute(`SELECT table_name FROM information_schema.tables WHERE table_schema='`~_dbname~`'`, (MySQLRow row) {
				output ~= row.table_name.get!string;
			});
			return inputRangeObject(output);
		}
		override void deleteDB(string name) {
			auto lock = database.lockConnection();
			lock.execute("DROP TABLE "~name);
		}
		override void deleteIDs(string dbname, ForwardRange!string range) {
			import std.string : format;
			import std.array : array;
			auto lock = database.lockConnection();
			lock.execute(format("DELETE FROM %s WHERE IDS IN ", dbname)~range.array.placeholders, range.array);
		}
		override void optimize() {
		}
		this(string host, ushort port, string user, string pass, string db) {
			_dbname = db;
			database = new MySQLClient(host, port, user, pass, db);
		}
		override void close() {}
		override InputRange!string containsIDs(in string dbname, ForwardRange!string range) {
			import std.string : format;
			import std.array : array;
			import std.range : inputRangeObject;
			string[] output;
			auto lock = database.lockConnection();
			lock.execute(format("SELECT IDS FROM %s WHERE IDS IN ", dbname)~range.array.placeholders, range.array, (MySQLRow row) {
				output ~= row.IDS.get!string;
			});
			return inputRangeObject(output);
		}
	}
	unittest {
		import std.parallelism;
		import std.process;
		import std.conv;
		import idstore.common : test;
		auto task1 = task!(test!(MySQL, string, ushort, string, string, string))("mysql", environment.get("MYHOST", "localhost"), environment.get("MYPORT", "3306").to!ushort, environment.get("MYUSER", "root"), environment.get("MYPASS", ""), environment.get("MYDB", "mysqltest"));
		task1.executeInNewThread();
		task1.yieldForce();
	}
}