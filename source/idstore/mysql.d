module idstore.mysql;
version(Have_mysql_lited) {
	private import idstore.common;
	private import std.range.interfaces;
	class MySQL : Database {
		import std.traits : ReturnType;
		import mysql;
		private ReturnType!(MySQLClient.lockConnection) database;
		string _dbname;

		override void createDB(string dbname) {
			database.execute("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS VARCHAR(255) PRIMARY KEY)");
		}
		override void insertIDs(in string dbname, ForwardRange!string range) {
			createDB(dbname);
			auto insert = inserter(database, dbname, "IDS");
			foreach (id; range)
				insert.row(id);
			insert.flush();
		}
		override ForwardRange!string listIDs(in string dbname) {
			import std.range : inputRangeObject;
			string[] output;
			database.execute(`SELECT IDS FROM '`~_dbname~`'`, (MySQLRow row) {
				output ~= row.IDS.get!string;
			});
			return inputRangeObject(output);
		}
		override ForwardRange!string listDBs() {
			import std.range : inputRangeObject;
			string[] output;
			database.execute(`SELECT table_name FROM information_schema.tables WHERE table_schema='`~_dbname~`'`, (MySQLRow row) {
				output ~= row.table_name.get!string;
			});
			return inputRangeObject(output);
		}
		override void deleteDB(string name) {
			database.execute("DROP TABLE "~name);
		}
		override void deleteIDs(string dbname, ForwardRange!string range) {
			import std.string : format;
			import std.array : array;
			database.execute(format("DELETE FROM %s WHERE IDS IN ", dbname)~range.array.placeholders, range.array);
		}
		override void optimize() {
		}
		this(string host, ushort port, string user, string pass, string db) {
			_dbname = db;
			auto client = new MySQLClient(host, port, user, pass, db);
			database = client.lockConnection();
		}
		override void close() {}
		override InputRange!string containsIDs(in string dbname, ForwardRange!string range) {
			import std.string : format;
			import std.array : array;
			import std.range : inputRangeObject;
			string[] output;
			database.execute(format("SELECT IDS FROM %s WHERE IDS IN ", dbname)~range.array.placeholders, range.array, (MySQLRow row) {
				output ~= row.IDS.get!string;
			});
			return inputRangeObject(output);
		}
	}
}