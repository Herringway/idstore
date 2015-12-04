module idstore.sqlite;
import std.stdio;
package enum depthLimit = 999;
package enum defaultFmtString = genFmtString(depthLimit);
package enum defaultFmtString2 = genFmtString2(depthLimit);
package struct Sqlite {
	import d2sqlite3 : Database, RowCache;
	private Database database;

	void createDB(string dbname) {
		database.execute("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS TEXT PRIMARY KEY)");
	}
	void insertIDs(T)(in string dbname, T range) {
		import std.range: chunks, enumerate, hasLength;
		import std.algorithm : min;
		import std.string : format;
		database.execute("BEGIN TRANSACTION");
		scope(failure) database.execute("ROLLBACK");
		scope(success) database.execute("COMMIT");
		createDB(dbname);

		auto fmtString = defaultFmtString2;
		static if (hasLength!T)
			fmtString = genFmtString2(min(range.length, depthLimit));
		auto query = database.prepare(format("INSERT INTO '%s' (IDS) VALUES %s", dbname, fmtString));
		foreach (idChunk; range.chunks(depthLimit)) {
			foreach (i, ID; idChunk.enumerate(1))
				query.bind(i, ID);
			query.execute();
			query.clearBindings();
			query.reset();
		}
	}
	auto listIDs(in string dbname) {
		string[] output;
		auto query = database.prepare("SELECT * from " ~ dbname);
		foreach (row; query.execute())
			output ~= row["IDS"].as!string;
		query.reset();
		return output;
	}
	auto listDBs() {
		string[] output;
		auto query = database.prepare(`SELECT name FROM sqlite_master WHERE type = "table"`);
		foreach (row; query.execute())
			output ~= row["name"].as!string;
		query.reset();
		return output;
	}
	void deleteDB(string name) {
		database.execute("DROP TABLE "~name);
	}
	void deleteIDs(T)(string dbname, T range) {
		import std.range: chunks, enumerate, hasLength;
		import std.algorithm : min;
		import std.string : format;
		database.execute("BEGIN TRANSACTION");
		scope (failure)	database.execute("ROLLBACK");
		scope (success) database.execute("COMMIT");
		auto fmtString = defaultFmtString;
		static if (hasLength!T)
			fmtString = genFmtString(min(range.length, depthLimit));
		auto query = database.prepare(format("DELETE FROM '%s' WHERE IDS IN (%s)", dbname, fmtString));
		foreach (idChunk; range.chunks(depthLimit)) {
			foreach (i, ID; idChunk.enumerate(1))
				query.bind(i, ID);
			query.execute();
			query.clearBindings();
			query.reset();
		}
	}
	void optimize() {
		database.execute("VACUUM");
	}
	this(string filename) {
		database = Database(filename);
	}
	void close() {
		database.close();
	}
	auto containsIDs(T)(in string dbname, T range) {
		import std.concurrency : Generator, yield;
		import std.range: ElementType, iota, chunks, enumerate, hasLength;
		import std.algorithm : map, min;
		import std.string : format;
		import std.array : array;
		return new Generator!(ElementType!T)( {
			auto fmtString = defaultFmtString;
			static if (hasLength!T)
				fmtString = genFmtString(min(range.length, depthLimit));
			auto query = database.prepare(format("SELECT * FROM %s WHERE IDS IN (%s) COLLATE NOCASE;", dbname, fmtString));
			foreach (idChunk; range.chunks(depthLimit)) {
				foreach (i, id; idChunk.enumerate(1))
					query.bind(i, id);
				auto results = RowCache(query.execute());
				foreach (row; results)
					yield(row["IDS"].as!string);
				query.clearBindings();
				query.reset();
			}
			destroy(query);
		});
	}
}
package dstring genFmtString(string Format = "?%d")(int count) {
	import std.range : iota;
	import std.algorithm : joiner, map;
	import std.string : format;
	import std.array : array;
	return iota(1, count+1).map!((int x) => format(Format, x)).joiner(",").array;
}
alias genFmtString2 = genFmtString!"(?%d)";