module idstore.common;

private import std.stdio;
private import std.range.interfaces;

private import idstore;

interface Database {
	void createDB(string dbname);
	void insertIDs(in string dbname, ForwardRange!string range);
	InputRange!string listIDs(in string dbname);
	InputRange!string listDBs();
	void deleteDB(string name);
	void deleteIDs(string dbname, ForwardRange!string range);
	void optimize();
	void close();
	InputRange!string containsIDs(in string dbname, ForwardRange!string range);
}
struct IDStore {
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
		database.deleteIDs(name, inputRangeObject(ids));
	}
	void deleteDB(string db) {
		database.deleteDB(db);
	}
	void insertID(T)(string name, T ids) if (isInputRange!T) {
		if (isDisabled)
			return;
		database.insertIDs(name, inputRangeObject(ids));
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
			return ReturnType!(Database.containsIDs).init;
		return database.containsIDs(name, inputRangeObject(ids));
	}
	void close() {
		database.close();
	}
}
IDStore openStore(T, U...)(U args) {
	auto output = IDStore();
	output.database = new T(args);
	return output;
}
IDStore openStore(string path) {
	return openStore!SQLite(path);
}
version(unittest) {
	void test(T)(string testid, T db) {
		import std.file : remove, exists;
		import std.datetime : benchmark, Duration;
		import std.range : iota, zip, enumerate;
		import std.array : array, empty;
		import std.algorithm : map, reduce, sort, setDifference, equal;
		import std.stdio : writeln, writefln;
		import std.conv : text, to;
		import std.exception : assertNotThrown, assertThrown;
		enum count = 1000;
		enum word = "testword";
		enum words1 = iota(0,count).map!text;
		enum words2 = iota(0,count).map!((a) => "word"~text(a));
		enum words3 = iota(0,count).map!((a) => "Nonexistant"~text(a));
		enum words4 = iota(0,count).map!((a) => "extra"~text(a));
		writeln("Beginning database test ", testid);

		void test1() {
			scope(failure)
				db.deleteDB("test");
			assert(word !in db["test"], "Found item in empty database");
			assert(words1 !in db["test"], "Found one of several items in empty database");
		}
		void test2() {
			scope(failure)
				db.deleteDB("test");
			db["test"] ~= word;

			assert(word in db["test"], "Single word not found in database");
			assert(equal(db.listDbs(), ["test"]), "Database list missing just-added database");
		}
		void test3() {
			scope(failure)
				db.deleteDB("test");
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
			scope(failure)
				db.deleteDB("test");
			assertNotThrown(db["test"].remove(words1), "Deletion 1 failed");
			assertNotThrown(db["test"].remove(words2), "Deletion 2 failed");
			assertNotThrown(db["test"].remove(words3), "Deletion of nonexistant ids failed");
		}
		void test5() {
			scope(failure)
				db.deleteDB("test");
			assert(words1 !in db["test"], "Deletion failed in words1");
			assert(words2 !in db["test"], "Deletion failed in words2");
		}
		void test6() {
			db.deleteDB("test");
		}
		auto times = benchmark!(test1, test2, test3, test4, test5, test6)(1)[].map!(to!Duration);
		foreach (i, time; times.enumerate(1))
			writefln("%s: Test %d completed in %s", testid, i, time);
		writefln("%s: Full test completed in %s", testid, times.reduce!((a,b) => a+b));
	}
}