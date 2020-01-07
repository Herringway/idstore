module idstore.common;

import std.stdio;
import std.range.interfaces;

import ddbc;

struct IDStore {
	import std.range : SortedRange, isInputRange;
	public bool isDisabled = false;
	private Connection database;
	alias opIndex = db;
	final public auto ref db(string dbname) {
		static struct DB {
			string name;
			IDStore db;
			this(IDStore store, string name) {
				this.name = name;
				db = store;
			}
			final void opOpAssign(string op)(string[] ids...) if (op == "~") {
				db.insertID(name, ids);
			}
			final void opOpAssign(string op, T)(T range) if (op == "~") {
				db.insertID(name, range);
			}
			final void remove(string[] ids...) {
				db.deleteID(name, ids);
			}
			final void remove(T)(T range) if (isInputRange!T) {
				db.deleteID(name, range);
			}
			final bool opBinaryRight(string op : "in")(string[] ids...) {
				return db.inDB(name, ids);
			}
			final bool opBinaryRight(string op :"in", T)(T range) if (isInputRange!T) {
				return db.inDB(name, range);
			}
			alias canFind = opBinaryRight!"in";
			final auto contains(string[] ids...) {
				return db.contains(name, ids);
			}
			final auto contains(T)(T range) if (isInputRange!T) {
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
	final private bool inDB(T)(string dbname, T range) if (isInputRange!T) {
		import std.array : empty;
		return !contains(dbname, range).empty;
	}
	void createDB(const string dbname) {
		if (isDisabled) {
			return;
		}
	    auto statement = database.createStatement();
	    scope(exit) statement.close();
		statement.executeUpdate("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS VARCHAR(127) PRIMARY KEY)");
	}
	void deleteID(T)(string name, T ids) if (isInputRange!T) {
		import std.string : format;
		import std.array : array;
		if (isDisabled) {
			return;
		}
		createDB(name);
		auto prepared = database.prepareStatement(format!"DELETE FROM `%s` WHERE IDS=?"(name));
		scope(exit) prepared.close();
		foreach (id; ids) {
			prepared.setString(1, id);
			prepared.executeUpdate();
		}
	}
	void deleteDB(const string name) {
	    auto statement = database.createStatement();
	    scope(exit) statement.close();
		statement.executeUpdate("DROP TABLE IF EXISTS "~name);
	}
	void insertID(T)(const string name, T ids) if (isInputRange!T) {
		import std.format : format;
		if (isDisabled) {
			return;
		}
		createDB(name);
		auto prepared = database.prepareStatement(format!"INSERT INTO `%s` (IDs) VALUES (?)"(name));
		scope(exit) prepared.close();
		foreach (id; ids) {
			prepared.setString(1, id);
			prepared.executeUpdate();
		}
	}
	string[] listIDs(const string dbname) {
		import std.format : format;
		import std.traits : ReturnType;
		if (isDisabled) {
			return typeof(return).init;
		}
		string[] output;
		createDB(dbname);
	    auto statement = database.createStatement();
	    scope(exit) statement.close();
		auto prepared = statement.executeQuery(format!"SELECT IDS FROM `%s`"(dbname));
		foreach (row; prepared) {
			output ~= row.getString(1);
		}
		return output;
	}
	string[] contains(T)(const string name, T range) if (isInputRange!T) {
		import std.array : array;
		import std.exception : enforce;
		import std.string : format;
		if (isDisabled) {
			return typeof(return).init;
		}
		string[] output;
		createDB(name);
		auto prepared = database.prepareStatement(format!"SELECT IDS FROM `%s` WHERE IDS=?"(name));
		scope(exit) prepared.close();
		foreach (id; range) {
			prepared.setString(1, id);
			auto res = prepared.executeQuery();
			while (res.next()) {
				output ~= res.getString(1);
				break;
			}
			res.next();
			enforce(res.isLast, "Multiple IDs found?");
		}
		return output;
	}
	void close() {
		database.close();
	}
	this(const string url) {
		database = createConnection(url);
	}
}
IDStore openStore(string path) {
	return IDStore(path);
}
unittest {
	import std.file : remove, exists;
	import std.datetime.stopwatch : benchmark;
	import std.datetime : Duration;
	import std.range : iota, zip, enumerate;
	import std.array : array, empty;
	import std.algorithm : map, reduce, sort, setDifference, equal;
	import std.stdio : writeln, writefln;
	import std.conv : text, to;
	import std.exception : assertNotThrown, assertThrown;
	import std.experimental.logger;
	enum count = 1000;
	enum word = "testword";
	enum words1 = iota(0,count).map!text;
	enum words2 = iota(0,count).map!((a) => "word"~text(a));
	enum words3 = iota(0,count).map!((a) => "Nonexistant"~text(a));
	enum words4 = iota(0,count).map!((a) => "extra"~text(a));
	info("Beginning database test");
	auto db = openStore("sqlite::memory:");
	void test1() {
		scope(failure) db.deleteDB("test");
		assert(word !in db["test"], "Found item in empty database");
		assert(words1 !in db["test"], "Found one of several items in empty database");
		info("test 1 complete");
	}
	void test2() {
		scope(failure) db.deleteDB("test");
		db["test"] ~= word;

		assert(word in db["test"], "Single word not found in database");
		info("test 2 complete");
	}
	void test3() {
		scope(failure) db.deleteDB("test");
		db["test"] ~= words1;
		db["test"] ~= words2;
		db["test"] ~= words4;
		assert(equal(db["test"].contains(words1).array.sort(), words1.array.sort()), "Missing ID from set1");
		assert(equal(db["test"].contains(words2).array.sort(), words2.array.sort()), "Missing ID from set2");
		assert(db["test"].contains(words3).empty, "Found ID from set3");
		assert(equal(db["test"].contains(words4).array.sort(), words4.array.sort()), "Missing ID from set4");
		info("test 3 complete");
	}
	void test4() {
		scope(failure) db.deleteDB("test");
		assertNotThrown(db["test"].remove(words1), "Deletion 1 failed");
		assertNotThrown(db["test"].remove(words2), "Deletion 2 failed");
		assertNotThrown(db["test"].remove(words3), "Deletion of nonexistant ids failed");
		info("test 4 complete");
	}
	void test5() {
		scope(failure) db.deleteDB("test");
		assert(words1 !in db["test"], "Deletion failed in words1");
		assert(words2 !in db["test"], "Deletion failed in words2");
		info("test 5 complete");
	}
	void test6() {
		db.deleteDB("test");
		info("test 6 complete");
	}
	auto times = benchmark!(test1, test2, test3, test4, test5, test6)(1)[].map!(to!Duration);
	foreach (i, time; times.enumerate(1)) {
		infof("Test %d completed in %s", i, time);
	}
	infof("Full test completed in %s", times.reduce!((a,b) => a+b));
}