module idstore.postgresql;
private import idstore.common;
private import std.range.interfaces;

version(Have_dpq2) {
	class PostgreSQL : Database {
		import std.conv : text;
		import dpq2;
		private Connection database;
		this(string host, ushort port, string user, string pass, string db) {
			database = new Connection("dbname="~db~" user="~user~" host="~host~" port="~port.text~" password="~pass);
		}

		override void createDB(string dbname) {
			database.exec("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS VARCHAR(255) PRIMARY KEY)");
		}
		override void insertIDs(in string dbname, ForwardRange!string range) {
			import std.algorithm : map;
			import std.array : array;
			import dpq2.conv.to_d_types : Bson;
			createDB(dbname);
			QueryParams query;
			query.sqlCommand = "INSERT INTO "~dbname~" (IDS) VALUES (unnest($1::text[]));";
			query.args.length = 1;
			query.args[0] = Bson(range.map!(x => Bson(x)).array).bsonToValue;
			database.execParams(query);
		}
		override ForwardRange!string listIDs(in string dbname) {
			import std.range : inputRangeObject;
			import std.traits: ReturnType;
			static struct Result {
				private Connection db;
				private size_t index;
				private ReturnType!(database.exec) result;
				auto save() {
					return this;
				}
				auto front() {
					return result[index]["IDS"].as!PGtext;
				}
				void popFront() {
					index++;
				}
				bool empty() {
					return index >= result.length;
				}
				this(Connection db_, string dbname_) {
					db = db_;
					result = db.exec("SELECT IDS FROM '"~dbname_~"'");
				}
			}
			return inputRangeObject(Result(database, dbname));
		}
		override ForwardRange!string listDBs() {
			import std.range : inputRangeObject;
			import std.traits : ReturnType;
			static struct Result {
				private Connection db;
				private size_t index;
				private ReturnType!(database.exec) result;
				auto save() {
					return this;
				}
				auto front() {
					return result[index]["table_name"].as!PGtext;
				}
				void popFront() {
					index++;
				}
				bool empty() {
					return index >= result.length;
				}
				this(Connection db_) {
					db = db_;
					result = db.exec("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';");
				}
			}
			return inputRangeObject(Result(database));
		}
		override void deleteDB(string name) {
			database.exec("DROP TABLE "~name);
		}
		override void deleteIDs(string dbname, ForwardRange!string range) {
			import std.algorithm : map;
			import std.array : array;
			import dpq2.conv.to_d_types : Bson;
			QueryParams query;
			query.sqlCommand = "DELETE FROM "~dbname~" WHERE IDS=ANY($1::text[]);";
			query.args.length = 1;
			query.args[0] = Bson(range.map!(x => Bson(x)).array).bsonToValue;
			database.execParams(query);
		}
		override void optimize() {}
		override void close() {}
		override ForwardRange!string containsIDs(in string dbname, ForwardRange!string range) {
			import dpq2.conv.to_d_types : Bson;
			import std.algorithm : map;
			import std.array : array;
			import std.range : inputRangeObject;
			import std.range : inputRangeObject;
			import std.string : assumeUTF;
			import std.traits : ReturnType;
			static struct Result {
				private Connection db;
				private size_t index;
				private ReturnType!(database.exec) result;
				auto save() {
					return this;
				}
				auto front() {
					return result[index]["IDS"].data.assumeUTF;
				}
				void popFront() {
					index++;
				}
				bool empty() {
					return index >= result.length;
				}
				this(Connection db_, ForwardRange!string range_, string dbname_) {
					QueryParams query;
					query.sqlCommand = "SELECT * FROM "~dbname_~" WHERE IDS=ANY($1::text[]);";
					query.args.length = 1;
					query.args[0] = Bson(range_.map!(x => Bson(x)).array).bsonToValue;
					db = db_;
					result = db.execParams(query);
				}
			}
			return inputRangeObject(Result(database, range, dbname));
		}
	}

	unittest {
		import std.parallelism;
		import std.process;
		import std.conv;
		import idstore.common : test;
		auto task1 = task!(test!(PostgreSQL, string, ushort, string, string, string))("postgres", environment.get("PGHOST", "localhost"), environment.get("PGPORT", "5432").to!ushort, environment.get("PGUSER", "postgres"), environment.get("PGPASS", ""), environment.get("PGDB", "postgrestest"));
		task1.executeInNewThread();
		task1.yieldForce();
	}
}