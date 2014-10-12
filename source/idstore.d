module idstore;

class IDStore {
	import d2sqlite3 : Database, Query;
	import std.range : SortedRange;
	final private class idlist (Range) {
		invariant() {
			if (!this.outer.isDisabled)
				assert((cast(Database)this.outer.database).handle !is null, "Database handle disappeared!");
		}
		Range range;
		string dbname;
		private size_t index = 0;
		private string[] resultbuffer;
		private auto depthLimit = 500;
		this(Range r, string db, ref Database sqlite) nothrow {
			import std.array : empty;
			range = r; dbname = db;
			while (!range.empty && (resultbuffer.length == 0))
				popFront();
		}
		void popFront() nothrow {
			import std.range : take, popFrontN;
			import std.array : popFront;
			import std.algorithm : map;
			import std.string : join, format;
			import std.stdio : writeln;
			index++;
			if (index >= resultbuffer.length) {
				resultbuffer = [];
				auto chunk = take(range, depthLimit).map!((x) => sanitizeData(x));
				range.popFrontN(chunk.length);
				index = 0;
				try {
					string query = format("SELECT * FROM %s WHERE IDS=%-(%s OR %)%s;", dbname, chunk, dbname == "md5" ? " COLLATE NOCASE" : "");
					auto q = this.outer.query(query);
					foreach (row; q.rows)
						resultbuffer ~= row.IDS.get!string;
					q.reset();
				} catch (Exception) {}
			}
		}
		@property {
			ref string front() nothrow {
				return resultbuffer[index];
			}
			bool empty() nothrow {
				import std.array : empty;
				return range.empty && (index >= resultbuffer.length);
			}
		}
	}
	final class DB {
		string db;
		this(string name) nothrow {
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
		final bool opIn_r (string[] ids...) nothrow {
			return this.outer.inDB(db, ids);
		}
		final bool opIn_r(T)(T range) nothrow {
			return this.outer.inDB(db, range);
		}
		final auto contains(string[] ids...) nothrow {
			return this.outer.contains(db, ids);
		}
		final auto contains(T)(T range) nothrow {
			return this.outer.contains(db, range);
		}
	}
	private Database database;
	invariant() {
		if (!isDisabled)
			assert((cast(Database)database).handle !is null, "Database handle disappeared!");
	}
	public bool isDisabled = false;
	private Query query(string inQuery) {
		return database.query(inQuery);
	}
	final @property ref DB opIndex(string s) nothrow {
		return db(s);
	}
	final void createDB(string dbname) {
		database.execute("CREATE TABLE IF NOT EXISTS " ~ dbname ~ " (IDS TEXT PRIMARY KEY)");
	}
	final private ref DB db(string dbname) nothrow {
		static DB[string] instances;
		if (dbname !in instances)
			instances[dbname] = this.new DB(dbname);
		return instances[dbname];
	}
	final private bool inDB(T)(string dbname, T range) nothrow {
		import std.exception;
		return assumeWontThrow(!contains(dbname, range).empty);
	}
	final private auto contains(T)(in string dbname, T range) nothrow {
		import std.exception;
		return assumeWontThrow(new idlist!T(range, dbname, database));
	}
	final private void insertID(T)(in string dbname, T range) nothrow {
		if (isDisabled)
			return;
		scope(failure) return;
		database.execute("BEGIN TRANSACTION");
		scope(failure) { 
			database.execute("ROLLBACK");
		}
		createDB(dbname);

		auto query = database.query("INSERT INTO '"~dbname~"' (IDS) VALUES (:ID)");
		foreach (ID; range) {
			query.bind(":ID", ID);
			query.execute();
			query.reset();
		}
		database.execute("COMMIT");
	}
	final auto listIDs(in string dbname) {
		string[] output;
		auto query = database.query("SELECT * from " ~ dbname);
		foreach (row; query.rows)
			output ~= row.IDS.get!string;
		query.reset();
		return output;
	}
	final auto listDbs() {
		string[] output;
		auto query = database.query(`SELECT name FROM sqlite_master WHERE type = "table"`);
		foreach (row; query.rows)
			output ~= row.name.get!string;
		query.reset();
		return output;
	}
	final private void deleteID(T)(string dbname, T IDs) {
		if (isDisabled)
			return;
		database.execute("BEGIN TRANSACTION");
		scope (failure)	database.execute("ROLLBACK");
		auto query = database.query("DELETE FROM " ~ dbname ~ " WHERE IDS=:ID");
		foreach (ID; IDs) {
			if (inDB(dbname, [ID])) {
				query.bind(":ID", ID);
				query.execute();
				query.reset();
			}
			else
				throw new NoDatabaseMatchException(ID, dbname);
		}
		database.execute("COMMIT");
	}
	this(string filename) {
		database = Database(filename);
	}
	void close() {
		isDisabled = true;
		destroy(database);
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
private @property string sanitizeData(T)(T input) pure {
	import std.array : replace;
	import std.string : isNumeric;
	import std.conv : text;
	auto str = text(input);
	if (str.isNumeric())
		return str;
	return "'" ~ replace(str, "'", "\'") ~ "'";
}
unittest {
	import std.file;
	import std.datetime;
	import std.range : iota, zip;
	import std.array : array;
	import std.algorithm : map, reduce;
	import std.stdio : writeln, writefln;
	import std.conv : text;
	import std.exception : assertNotThrown, assertThrown;
	enum testFilename = ":memory:"; //in-memory
	//enum testFilename = "test.db"; //file
	scope(exit) if (exists(testFilename)) remove(testFilename);
	enum words1 = map!text(iota(0,100));
	enum words2 = map!((a) => "word"~text(a))(iota(0,100));
	enum words3 = map!((a) => "Nonexistant"~text(a))(iota(0,100));
	enum words4 = map!((a) => "extra"~text(a))(iota(0,100));
	writeln("Beginning database test");
	StopWatch timer;
	long[] times;
	timer.start();
	auto db = new IDStore(testFilename);
	foreach (word; words1)
		db["test"] ~= word;
	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database Insertion 1 completed in %sms", times[$-1]);
	assert(db.listDbs() == ["test"], "Database list missing just-added database");
	assert(db.listIDs("test") == array(words1), "Missing ids in list");
	timer.reset();
	timer.start();
	foreach (word; words2)
		db["test"] ~= word;
	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database Insertion 2 completed in %sms", times[$-1]);
	timer.reset();
	timer.start();
	db["test"] ~= words4;
	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database Insertion 3 completed in %sms", times[$-1]);
	timer.reset();
	timer.start();
	foreach (word1, word2, word3; zip(words1, words2, words3)) {
		assert(word1  in db["test"], "Missing ID: " ~ word1);
		assert(word2  in db["test"], "Missing ID: " ~ word2);
		assert(word3 !in db["test"], "Found ID: " ~ word3);
	}
	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database ID check completed in %sms", times[$-1]);
	timer.reset();
	timer.start();
	assert(words1 in db["test"], "Missing ID from set1");
	assert(words2 in db["test"], "Missing ID from set2");
	assert(words4 in db["test"], "Missing ID from set4");
	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database ID check (ranges) completed in %sms", times[$-1]);
	timer.reset();
	timer.start();
	//foreach (word1, word2, word3; zip(words1, words2, words3)) {
		assertNotThrown(db["test"].remove(words1), "Deletion 1 failed");
		assertNotThrown(db["test"].remove(words2), "Deletion 2 failed");
		assertThrown   (db["test"].remove(words3), "Apparent success deleting nonexistant IDs");
	//}
	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database ID deletion completed in %sms", times[$-1]);
	timer.reset();
	timer.start();
	foreach (word1, word2; zip(words1, words2)) {
		assert(word1 !in db["test"], "Found deleted ID: " ~ word1);
		assert(word2 !in db["test"], "Found deleted ID: word" ~ word2);
	}
	timer.stop();
	times ~= timer.peek().msecs;
	writefln("Database post-deletion ID check completed in %sms", times[$-1]);
	writefln("Database test completed in %sms", reduce!((a,b) => a+b)(cast(ulong)0, times));
	db.close();

}