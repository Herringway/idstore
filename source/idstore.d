module idstore;

import std.stdio;
struct IDStore {
	import d2sqlite3 : Database, Statement, RowCache;
	import std.range : SortedRange;
	private Database database;
	public bool isDisabled = false;
	alias opIndex = db;
	final void createDB(string dbname) {
		database.execute("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS TEXT PRIMARY KEY)");
	}
	final public auto ref db(string dbname) {
		struct DB {
			string name;
			IDStore db;
			this(IDStore store, string name) {
				this.name = name;
				db = store;
			}
			final void opOpAssign(string op)(string[] ids...) nothrow if (op == "~") {
				db.insertID(name, ids);
			}
			final void opOpAssign(string op, T)(T range) nothrow if (op == "~") {
				db.insertID(name, range);
			}
			final void remove(string[] ids...) {
				db.deleteID(name, ids);
			}
			final void remove(T)(T range) {
				db.deleteID(name, range);
			}
			final bool opIn_r(string[] ids...) {
				return db.inDB(name, ids);
			}
			final bool opIn_r(T)(T range) {
				return db.inDB(name, range);
			}
			final auto contains(string[] ids...) {
				return db.contains(name, ids);
			}
			final auto contains(T)(T range) {
				return db.contains(name, range);
			}
		}
		static DB[string] instances;
		if (dbname !in instances) {
			createDB(dbname);
			instances[dbname] = DB(this, dbname);
		}
		return instances[dbname];
	}
	final private bool inDB(T)(string dbname, T range) {
		return !contains(dbname, range).empty;
	}
	final private auto contains(T)(in string dbname, T range) {
		import std.concurrency : Generator, yield;
		import std.range: ElementType, iota, chunks, enumerate;
		import std.algorithm : map, min;
		import std.string : format;
		enum depthLimit = 500;
		return new Generator!(ElementType!T)( {
			auto count = depthLimit;
			static if (__traits(compiles, r.length))
				count = min(r.length, count);
			auto query = database.prepare(format("SELECT * FROM %s WHERE IDS IN (%-(%s,%)) COLLATE NOCASE;", dbname, iota(0, count).map!((a) => format(":P%04d", a))));
			foreach (idChunk; range.chunks(depthLimit)) {
				foreach (i, id; idChunk.enumerate)
					query.bind(format(":P%04d", i), id);
				auto results = RowCache(query.execute());
				foreach (row; results)
					yield(row["IDS"].as!string);
				query.reset();
			}
			destroy(query);
		});
	}
	final private void insertID(T)(in string dbname, T range) nothrow {
		if (isDisabled)
			return;
		scope(failure) return;
		database.execute("BEGIN TRANSACTION");
		scope(failure) database.execute("ROLLBACK");
		scope(success) database.execute("COMMIT");
		createDB(dbname);

		auto query = database.prepare("INSERT INTO '"~dbname~"' (IDS) VALUES (:ID)");

		foreach (ID; range) {
			query.bind(":ID", ID);
			query.execute();
			query.reset();
		}
	}
	final auto listIDs(in string dbname) {
		string[] output;
		auto query = database.prepare("SELECT * from " ~ dbname);
		foreach (row; query.execute())
			output ~= row["IDS"].as!string;
		query.reset();
		return output;
	}
	final auto listDbs() {
		string[] output;
		auto query = database.prepare(`SELECT name FROM sqlite_master WHERE type = "table"`);
		foreach (row; query.execute())
			output ~= row["name"].as!string;
		query.reset();
		return output;
	}
	final void deleteDB(string name) {
		database.execute("DROP TABLE "~name);
	}
	final private void deleteID(T)(string dbname, T IDs) {
		if (isDisabled)
			return;
		database.execute("BEGIN TRANSACTION");
		scope (failure)	database.execute("ROLLBACK");
		scope (success) database.execute("COMMIT");
		auto query = database.prepare("DELETE FROM " ~ dbname ~ " WHERE IDS=:ID");
		foreach (ID; IDs) {
			query.bind(":ID", ID);
			query.execute();
			query.reset();
		}
	}
	final public void optimize() {
		auto disablestate = isDisabled;
		isDisabled = true;
		scope(exit) isDisabled = disablestate;
		database.execute("VACUUM");
	}
	this(string filename) {
		database = Database(filename);
	}
	final void close() {
		import d2sqlite3 : shutdown;
		isDisabled = true;
		database.close();
	}
}
auto openStore(string path) {
	return IDStore(path);
}
unittest {
	import std.file;
	import std.datetime;
	import std.range : iota, zip;
	import std.array : array;
	import std.algorithm : map, reduce, sort, setDifference, equal;
	import std.stdio : writeln, writefln;
	import std.conv : text;
	import std.exception : assertNotThrown, assertThrown;
	import d2sqlite3 : versionString;
	enum testFilename = ":memory:"; //in-memory
	//enum testFilename = "test.db"; //file
	writefln("Powered by sqlite %s", versionString);
	scope(exit) if (exists(testFilename)) remove(testFilename);
	enum count = 1000;
	enum words1 = iota(0,count).map!text;
	enum words2 = iota(0,count).map!((a) => "word"~text(a));
	enum words3 = iota(0,count).map!((a) => "Nonexistant"~text(a));
	enum words4 = iota(0,count).map!((a) => "extra"~text(a));
	writeln("Beginning database test");
	StopWatch timer;
	long[] times;
	auto db = openStore(testFilename);
	foreach (database; db.listDbs)
		db.deleteDB(database);
	timer.start();

	assert(words1 !in db["test"], "Found item in empty database");

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Empty database search completed in %sms", times[$-1]);
	timer.reset();
	timer.start();

	foreach (word; words1)
		db["test"] ~= word;

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database Insertion 1 (one-by-one) completed in %sms", times[$-1]);
	assert(db.listDbs() == ["test"], "Database list missing just-added database");
	assert(db.listIDs("test") == array(words1), "Missing ids in list");
	timer.reset();
	timer.start();

	foreach (word; words2)
		db["test"] ~= word;

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database Insertion 2 (one-by-one 2) completed in %sms", times[$-1]);
	timer.reset();
	timer.start();

	db["test"] ~= words4;

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database Insertion 3 (range) completed in %sms", times[$-1]);
	timer.reset();
	timer.start();

	foreach (word1, word2, word3; zip(words1, words2, words3)) {
		assert(word1  in db["test"], "Missing ID: " ~ word1);
		assert(word2  in db["test"], "Missing ID: " ~ word2);
		assert(word3 !in db["test"], "Found ID: " ~ word3);
	}

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database ID check (one-by-one) completed in %sms", times[$-1]);
	timer.reset();
	db.optimize();
	timer.start();

	assert(words1 in db["test"], "Missing ID from set1");
	assert(words2 in db["test"], "Missing ID from set2");
	assert(words4 in db["test"], "Missing ID from set4");

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database ID check (ranges) completed in %sms", times[$-1]);
	timer.reset();
	timer.start();
	assert(equal(db["test"].contains(words1).array.sort(), words1.array.sort()), "Missing ID from set1");
	assert(equal(db["test"].contains(words2).array.sort(), words2.array.sort()), "Missing ID from set2");
	assert(equal(db["test"].contains(words4).array.sort(), words4.array.sort()), "Missing ID from set4");

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database ID check (ranges, contains) completed in %sms", times[$-1]);
	timer.reset();
	timer.start();

	assertNotThrown(db["test"].remove(words1), "Deletion 1 failed");
	assertNotThrown(db["test"].remove(words2), "Deletion 2 failed");
	assertNotThrown(db["test"].remove(words3), "Deletion of nonexistant ids failed");

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database ID deletion completed in %sms", times[$-1]);
	timer.reset();
	timer.start();

	assert(words1 !in db["test"], "Deletion failed in words1");
	assert(words2 !in db["test"], "Deletion failed in words2");

	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database post-deletion ID check completed in %sms", times[$-1]);
	writefln("Database test completed in %sms", reduce!((a,b) => a+b)(cast(ulong)0, times));
	db.close();
	writeln("Database closed");
}