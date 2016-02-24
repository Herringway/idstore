module idstore.common;

import std.stdio;
struct IDStore(Database) {
	import std.range : SortedRange, isInputRange;
	public bool isDisabled = false;
	private Database database;
	alias opIndex = db;
	final public auto ref db(string dbname) {
		struct DB {
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
			final bool opIn_r(string[] ids...) {
				return db.inDB(name, ids);
			}
			final bool opIn_r(T)(T range) if (isInputRange!T) {
				return db.inDB(name, range);
			}
			alias canFind = opIn_r;
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
	void createDB(string name) {
		if (isDisabled)
			return;
		database.createDB(name);
	}
	void deleteID(T)(string name, T ids) if (isInputRange!T) {
		if (isDisabled)
			return;
		database.deleteIDs(name, ids);
	}
	void deleteDB(string db) {
		database.deleteDB(db);
	}
	void insertID(T)(string name, T ids) if (isInputRange!T) {
		if (isDisabled)
			return;
		database.insertIDs(name, ids);
	}
	void optimize() {
		if (isDisabled)
			return;
		database.optimize();
	}
	auto listDbs() {
		import std.traits : ReturnType;
		if (isDisabled)
			return ReturnType!(Database.listDBs).init;
		return database.listDBs();
	}
	bool opIn_r(string db) {
		import std.algorithm : canFind;
		return listDbs.canFind(db);
	}
	auto listIDs(string db) {
		import std.traits : ReturnType;
		if (isDisabled)
			return ReturnType!(Database.listIDs).init;
		return database.listIDs(db);
	}
	auto contains(T)(string name, T ids) if (isInputRange!T) {
		import std.traits : ReturnType;
		if (isDisabled)
			return ReturnType!(Database.containsIDs!T).init;
		return database.containsIDs(name, ids);
	}
	this(T...)(T args) {
		database = Database(args);
	}
	void close() {
		database.close();
	}
}
auto openStore(string path) {
	import idstore.sqlite;
	return IDStore!Sqlite(path);
}
version(unittest) {
	void test(uint testid, string filename) {
		import std.file : remove, exists;
		import std.datetime : benchmark, Duration;
		import std.range : iota, zip, enumerate;
		import std.array : array;
		import std.algorithm : map, reduce, sort, setDifference, equal;
		import std.stdio : writeln, writefln;
		import std.conv : text, to;
		import std.exception : assertNotThrown, assertThrown;
		scope(exit) if (exists(filename)) remove(filename);
		enum count = 1000;
		enum word = "testword";
		enum words1 = iota(0,count).map!text;
		enum words2 = iota(0,count).map!((a) => "word"~text(a));
		enum words3 = iota(0,count).map!((a) => "Nonexistant"~text(a));
		enum words4 = iota(0,count).map!((a) => "extra"~text(a));
		writeln("Beginning database test ", testid);
		auto db = openStore(filename);

		void test1() {
			assert(words1 !in db["test"], "Found item in empty database");
		}
		void test2() {
			db["test"] ~= word;

			assert(word in db["test"], "Single word not found in database");
			assert(db.listDbs() == ["test"], "Database list missing just-added database");
		}
		void test3() {
			db["test"] ~= words1;
			db["test"] ~= words2;
			db["test"] ~= words4;
			assertNotThrown(db.optimize(), "Optimization failure");
			assert(equal(db["test"].contains(words1).array.sort(), words1.array.sort()), "Missing ID from set1");
			assert(equal(db["test"].contains(words2).array.sort(), words2.array.sort()), "Missing ID from set2");
			assert(db["test"].contains(words3).empty, "Found ID from set3");
			assert(equal(db["test"].contains(words4).array.sort(), words4.array.sort()), "Missing ID from set4");
		}
		void test4() {
			assertNotThrown(db["test"].remove(words1), "Deletion 1 failed");
			assertNotThrown(db["test"].remove(words2), "Deletion 2 failed");
			assertNotThrown(db["test"].remove(words3), "Deletion of nonexistant ids failed");
		}
		void test5() {
			assert(words1 !in db["test"], "Deletion failed in words1");
			assert(words2 !in db["test"], "Deletion failed in words2");
		}
		auto times = benchmark!(test1, test2, test3, test4, test5)(1)[].map!(to!Duration);
		foreach (i, time; times.enumerate(1))
			writefln("Test %d completed in %s", i, time);
		writefln("Full test completed in %s on thread %s", times.reduce!((a,b) => a+b), testid);
	}
}
unittest {
	import std.parallelism;
	auto task1 = task!test(1, ":memory:"); //in-memory test
	task1.executeInNewThread();
	task1.yieldForce();
}