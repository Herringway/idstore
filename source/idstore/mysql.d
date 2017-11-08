module idstore.mysql;
version(Have_mysql_native) {
	private import idstore.common;
	private import std.range.interfaces;
	class MySQL : Database {
		import std.traits : ReturnType;
		import mysql;
		private Connection database;
		string _dbname;

		override void createDB(const string dbname) {
			database.exec("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS VARCHAR(127) PRIMARY KEY)");
		}
		override void insertIDs(const string dbname, ForwardRange!string range) {
			import std.format : format;
			createDB(dbname);
			auto prepared = database.prepare(format!"INSERT INTO `%s` (IDs) VALUES (?)"(dbname));
			foreach (id; range) {
				prepared.setArgs(id);
				prepared.exec();
			}
		}
		override ForwardRange!string listIDs(const string dbname) {
			import std.range : inputRangeObject;
			import std.format : format;
			string[] output;
			createDB(dbname);
			auto prepared = database.prepare(format!"SELECT IDS FROM `%s`"(dbname));
			foreach (row; prepared.query()) {
				output ~= row[0].get!string;
			}
			return inputRangeObject(output);
		}
		override ForwardRange!string listDBs() {
			import std.range : inputRangeObject;
			string[] output;
			auto prepared = database.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema=?");
			prepared.setArgs(_dbname);
			foreach (row; prepared.query()) {
				output ~= row[0].get!string;
			}
			return inputRangeObject(output);
		}
		override void deleteDB(const string name) {
			database.exec("DROP TABLE IF EXISTS "~name);
		}
		override void deleteIDs(const string dbname, ForwardRange!string range) {
			import std.string : format;
			import std.array : array;
			createDB(dbname);
			auto prepared = database.prepare(format!"DELETE FROM `%s` WHERE IDS=?"(dbname));
			foreach (id; range) {
				prepared.setArgs(id);
				prepared.exec();
			}
		}
		override void optimize() {
		}
		this(const string host, const ushort port, const string user, const string pass, const string db) {
			_dbname = db;
			database = new Connection(host, user, pass, db, port);
		}
		override void close() {
			database.close();
		}
		override ForwardRange!string containsIDs(const string dbname, ForwardRange!string range) {
			import std.string : format;
			import std.array : array;
			import std.range : inputRangeObject;
			string[] output;
			createDB(dbname);
			auto prepared = database.prepare(format!"SELECT IDS FROM `%s` WHERE IDS=?"(dbname));
			foreach (id; range) {
				prepared.setArgs(id);
				auto res = prepared.query();
				if (!res.empty) {
					output ~= res.front[0].get!string;
				}
			}
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