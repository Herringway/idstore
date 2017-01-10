module idstore.postgresql;
private import idstore.common;
private import std.range.interfaces;

version(Have_dpq2) {
	class PostgreSQL : Database {
		import std.conv : text;
		import dpq2;
		private Connection database;
		string _dbname;
		this(string host, ushort port, string user, string pass, string db) {
			_dbname = db;
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
		override InputRange!string listIDs(in string dbname) {
			import std.range : inputRangeObject;
			import std.concurrency : Generator, yield;
			return inputRangeObject(new Generator!string( {
				auto result = database.exec("SELECT IDS FROM '"~_dbname~"'");
				foreach (val; 0..result.length) {
					yield(result[val]["IDS"].as!PGtext);
				}
			}));
		}
		override InputRange!string listDBs() {
			import std.range : inputRangeObject;
			import std.concurrency : Generator, yield;
			return inputRangeObject(new Generator!string( {
				auto result = database.exec("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';");
				foreach (val; 0..result.length) {
					yield(result[val]["table_name"].as!PGtext);
				}
			}));
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
		override void optimize() {
		}
		override void close() {}
		override InputRange!string containsIDs(in string dbname, ForwardRange!string range) {
			import std.range : inputRangeObject;
			import std.concurrency : Generator, yield;
			import std.range: ElementType, iota, chunks, enumerate, hasLength;
			import std.algorithm : map, min;
			import std.string : format, assumeUTF;
			import std.array : array;
			import dpq2.conv.to_d_types : Bson;
			return inputRangeObject(new Generator!(ElementType!(typeof(range)))( {
				QueryParams query;
				query.sqlCommand = "SELECT * FROM "~dbname~" WHERE IDS=ANY($1::text[]);";
				query.args.length = 1;
				query.args[0] = Bson(range.map!(x => Bson(x)).array).bsonToValue;
				auto result = database.execParams(query);
				foreach (id; 0..result.length) {
					yield(result[id]["IDS"].data.assumeUTF);
				}
			}));
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