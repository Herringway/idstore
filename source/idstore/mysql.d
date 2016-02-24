module idstore.mysql;
version(Have_mysql_lited) { 
	package struct Mysql {
		import std.traits : ReturnType;
		import mysql;
		private ReturnType!(MySQLClient.lockConnection) database;
		string _dbname;

		void createDB(string dbname) {
			database.execute("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS VARCHAR(255) PRIMARY KEY)");
		}
		void insertIDs(T)(in string dbname, T range) {
			createDB(dbname);
			auto insert = inserter(database, dbname, "IDS");
			foreach (id; range)
				insert.row(id);
			insert.flush();
		}
		auto listIDs(in string dbname) {
			string[] output;
			database.execute(`SELECT IDS FROM '`~_dbname~`'`, (MySQLRow row) {
				output ~= row.IDS.get!string;
			});
			return output;
		}
		auto listDBs() {
			string[] output;
			database.execute(`SELECT table_name FROM information_schema.tables WHERE table_schema='`~_dbname~`'`, (MySQLRow row) {
				output ~= row.table_name.get!string;
			});
			return output;
		}
		void deleteDB(string name) {
			database.execute("DROP TABLE "~name);
		}
		void deleteIDs(T)(string dbname, T range) {
			import std.string : format;
			import std.array : array;
			database.execute(format("DELETE FROM %s WHERE IDS IN ", dbname)~range.array.placeholders, range.array);
		}
		void optimize() {
		}
		this(string host, string user, string pass, string db, ushort port = 3306) {
			_dbname = db;
			auto client = new MySQLClient(host, port, user, pass, db);
			database = client.lockConnection();
		}
		void close() {}
		auto containsIDs(T)(in string dbname, T range) {
			import std.string : format;
			import std.array : array;
			string[] output;
			database.execute(format("SELECT IDS FROM %s WHERE IDS IN ", dbname)~range.array.placeholders, range.array, (MySQLRow row) {
				output ~= row.IDS.get!string;
			});
			return output;
		}
	}
}