module idstore;

import std.stdio;
class IDStore {
	import d2sqlite3 : Database, Statement;
	import std.range : SortedRange;
	final private struct idlist (Range) {
		struct sqlite_buffer {
			import std.string : format;
			import std.range;
			import std.algorithm : map;
			int count;
			private Statement query;
			Range ids;
			this(int inCount, string db, ref Range inIDs, Database database) {
				count = inCount;
				ids = inIDs;
				query = database.prepare(format("SELECT * FROM %s WHERE IDS IN (%-(%s,%)) COLLATE NOCASE;", db, iota(0, count).map!((a) => format(":P%04d", a))));
			}
			string[] fetchNext() {
				string[] output;
				query.clearBindings();
				foreach (i; 0..count) {
					if (ids.empty)
						break;
					query.bind(format(":P%04d", i), ids.front);
					ids.popFront();
				}
				auto finishedQuery = query.execute();
				if (finishedQuery.empty)
					return output;
				foreach (row; finishedQuery)
					output ~= row["IDS"].as!string;
				query.reset();
				return output;
			}
			~this() {
				destroy(query);
			}
		}
		private size_t index = 0;
		private string[] resultbuffer;
		private auto depthLimit = 500;
		sqlite_buffer buffer;
		this(Range r, string db, ref Database sqlite) {
			auto count = depthLimit;
			static if (__traits(compiles, r.length)) {
				import std.algorithm : min;
				count = min(r.length, count);
			}
			buffer = sqlite_buffer(count, db, r, sqlite);
			resultbuffer = buffer.fetchNext();
		}
		void popFront() {
			index++;
			if (index >= resultbuffer.length) {
				resultbuffer = buffer.fetchNext();
				if (resultbuffer.length > 0)
					index = 0;
			}
		}
		@property {
			ref string front() nothrow {
				return resultbuffer[index];
			}
			bool empty() nothrow {
				return index >= resultbuffer.length;
			}
		}
	}
	final class DB {
		string db;
		this(string name) {
			this.outer.createDB(name);
			db = name;
		}
		final void opOpAssign(string op)(string[] ids...) nothrow if (op == "~") {
			this.outer.insertID(db, ids);
		}
		final void opOpAssign(string op, T)(T range) nothrow if (op == "~") {
			this.outer.insertID(db, range);
		}
		final void remove(string[] ids...) {
			this.outer.deleteID(db, ids);
		}
		final void remove(T)(T range) {
			this.outer.deleteID(db, range);
		}
		final bool opIn_r (string[] ids...) {
			return this.outer.inDB(db, ids);
		}
		final bool opIn_r(T)(T range) {
			return this.outer.inDB(db, range);
		}
		final auto contains(string[] ids...) {
			return this.outer.contains(db, ids);
		}
		final auto contains(T)(T range) {
			return this.outer.contains(db, range);
		}
	}
	private Database database;
	invariant() {
		if (!isDisabled)
			assert((cast(Database)database).handle !is null, "Database handle disappeared!");
	}
	public bool isDisabled = false;
	final @property ref DB opIndex(string s) {
		return db(s);
	}
	final void createDB(string dbname) {
		database.execute("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS TEXT PRIMARY KEY)");
	}
	final private ref DB db(string dbname) {
		static DB[string] instances;
		if (dbname !in instances)
			instances[dbname] = this.new DB(dbname);
		return instances[dbname];
	}
	final private bool inDB(T)(string dbname, T range) {
		return !contains(dbname, range).empty;
	}
	final private auto contains(T)(in string dbname, T range) {
		return idlist!T(range, dbname, database);
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
		shutdown();
	}
}
class IDAlreadyExistsException : Exception {
	import std.string : format;
	string pid;
	string id;
	this(string inID, string inPID) {
		pid = inPID;
		id = inID;
		super(format("%s already exists in %s database!", id, pid));
	}
}
class NoDatabaseMatchException : Exception {
	import std.string : format;
	string pid;
	string id;
	this(string inID, string inPID) {
		pid = inPID;
		id = inID;
		super(format("Could not find %s in %s database!", id, pid));
	}
}
unittest {
	import std.file;
	import std.datetime;
	import std.range : iota, zip;
	import std.array : array;
	import std.algorithm : map, reduce, sort, setDifference;
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
	auto db = new IDStore(testFilename);
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
	
	assert(array(sort(array(db["test"].contains(words1)))) == array(sort(array(words1))), "Missing ID from set1");
	assert(array(sort(array(db["test"].contains(words2)))) == array(sort(array(words2))), "Missing ID from set2");
	assert(array(sort(array(db["test"].contains(words4)))) == array(sort(array(words4))), "Missing ID from set4");
	
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