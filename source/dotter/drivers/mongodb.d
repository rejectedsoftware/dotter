/**
	MongoDB based mapping driver.

	Note that this module requires the vibe-d dependency to be present.

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.drivers.mongodb;

version (Have_vibe_d):

import dotter.orm;

import std.traits;
import std.typetuple;
import vibe.data.serialization;
import vibe.db.mongo.mongo;


/** ORM driver using MongoDB for data storage and query execution.

	The driver generates static types used to efficiently and directly
	serialize query expressions to BSON without unnecessary memory allocations.
*/
class MongoDBDriver(TABLES) {
	alias Tables = TABLES;
	alias TableTypes = TypeTuple!(typeof(TABLES.tupleof));

	private {
		MongoDatabase m_db;
		MongoCollection[TableTypes.length] m_collections;
	}

	alias DefaultID = BsonObjectID;
	alias TableHandle = MongoCollection;
	alias ColumnHandle = string;
	enum bool supportsArrays = true;

	this(MongoDatabase db)
	{
		m_db = db;

		foreach (i, tname; __traits(allMembers, TABLES)) {
			m_collections[i] = m_db[tname];
			// TODO: setup keys!
		}
	}

	this(string url_or_host, string name)
	{
		auto cli = connectMongoDB(url_or_host);
		this(cli.getDatabase(name));
	}
	
	auto find(T, QUERY)(QUERY query)
	{
		struct Query { mixin MongoQuery!(0, QUERY); }
		Query mquery;
		mixin(initializeMongoQuery!(0, QUERY)("mquery", "query"));
		
		import vibe.core.log; import vibe.data.bson;
		//logInfo("QUERY (%s): %s", table.name, serializeToBson(mquery).toString());
		
		return coll!T.find(mquery).map!(b => deserializeBson!(RawRow!T)(b));
	}

	void update(T, QUERY, UPDATE)(QUERY query, UPDATE update)
	{
		struct Query { mixin MongoQuery!(0, QUERY); }
		Query mquery;
		mixin(initializeMongoQuery!(0, QUERY)("mquery", "query"));

		struct Update { mixin MongoUpdate!(0, UPDATE); }
		Update mupdate;
		mixin(initializeMongoUpdate!(0, UPDATE)("mupdate", "update"));

		import vibe.core.log; import vibe.data.bson;
		//logInfo("QUERY (%s): %s", table.name, serializeToBson(mquery).toString());
		//logInfo("UPDATE: %s", serializeToBson(mupdate).toString());

		coll!T.update(mquery, mupdate);
	}

	void insert(T)(T value)
	{
		coll!(T.Table).insert(value);
	}

	void updateOrInsert(T, QUERY)(QUERY query, T value)
	{
		assert(false);
	}

	void removeAll(TABLE)()
	{
		coll!TABLE.remove(Bson.emptyObject);
	}

	private ref MongoCollection coll(TABLE)() { return m_collections[staticIndexOf!(TABLE, TableTypes)]; }
}

unittest {
	import dotter.test;
	static auto createDriver(TABLES)(MongoDatabase db) { return new MongoDBDriver!(TABLES)(db); }

	MongoDatabase db;
	try db = connectMongoDB("localhost").getDatabase("test");
	catch (Exception e) {
		import vibe.core.log;
		logWarn("Failed to connect to local MongoDB server. Skipping test.");
		return;
	}
	testDriver!(createDriver)(db);
}

private mixin template MongoQuery(size_t idx, QUERIES...) {
	static if (QUERIES.length > 1) {
		mixin MongoQuery!(idx, QUERIES[0 .. $/2]);
		mixin MongoQuery!(idx + QUERIES.length/2, QUERIES[$/2 .. $]);
	} else static if (QUERIES.length == 1) {
		static assert(!is(typeof(QUERIES[0])) || is(typeof(QUERIES[0])), "Arguments to MongoQuery must be types.");
		alias Q = QUERIES[0];

		static if (isInstanceOf!(CompareExpr, Q)) {
			static if (Q.op == CompareOp.equal || Q.op == CompareOp.contains) mixin("Q.V "~Q.name~";");
			else static if (Q.op == CompareOp.notEqual) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$ne")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.op == CompareOp.greater) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$gt")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.op == CompareOp.greaterEqual) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$gte")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.op == CompareOp.less) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$lt")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.op == CompareOp.lessEqual) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$lte")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static assert(false, format("Unsupported compare operator: %s", Q.op));
		} else static if (isInstanceOf!(ConjunctionExpr, Q)) {
			//mixin(format(`static struct Q%s { mixin MongoQuery!(0, Q.exprs); } @(vibe.data.serialization.name("$and")) Q%s q%s;`, idx, idx, idx));
			mixin MongoQuery!(0, typeof(Q.exprs));
		} else static if (isInstanceOf!(DisjunctionExpr, Q)) {
			mixin(format(
				q{static struct Q%s { mixin MongoQueries!(0, typeof(Q.exprs)); } @(vibe.data.serialization.name("$or"), vibe.data.serialization.asArray) Q%s q%s;}, idx, idx, idx));
		} else static if (is(Q == QueryAnyExpr)) {
			// empty query
		} else static assert(false, "Unsupported query expression type: "~Q.stringof);
	}
}

private mixin template MongoQueries(size_t idx, QUERIES...) {
	static if (QUERIES.length > 1) {
		mixin MongoQueries!(idx, QUERIES[0 .. $/2]);
		mixin MongoQueries!(idx + QUERIES.length/2, QUERIES[$/2 .. $]);
	} else static if (QUERIES.length == 1) {
		mixin(format(`struct Q%s { mixin MongoQuery!(0, QUERIES[0]); } Q%s q%s;`, idx, idx, idx));
	}
}

private static string initializeMongoQuery(size_t idx, QUERY)(string name, string srcfield)
{
	string ret;
	alias Q = QUERY;

	static if (isInstanceOf!(CompareExpr, Q)) {
		final switch (Q.op) with (CompareOp) {
			case equal:
			case contains:
				ret ~= format("%s.%s = %s.value;", name, Q.name, srcfield);
				break;
			case notEqual, greater, greaterEqual, less, lessEqual:
			//case contains:
				ret ~= format("%s.%s.value = %s.value;", name, Q.name, srcfield);
				break;
			case anyOf: assert(false, "TODO!");
		}
	} else static if (isInstanceOf!(ConjunctionExpr, Q)) {
		foreach (i, E; typeof(Q.exprs))
			//ret ~= initializeMongoQuery!(i, E)(format("%s.q%s", name, idx), format("%s.exprs[%s]", srcfield, i));
			ret ~= initializeMongoQuery!(i, E)(name, format("%s.exprs[%s]", srcfield, i));
	} else static if (isInstanceOf!(DisjunctionExpr, Q)) {
		foreach (i, E; typeof(Q.exprs))
			ret ~= initializeMongoQuery!(i, E)(format("%s.q%s.q%s", name, idx, i), format("%s.exprs[%s]", srcfield, i));
	} else static if (is(Q == QueryAnyExpr)) {
		// nothing to initialize;
	} else static assert(false, "Unsupported query expression type: "~Q.stringof);

	return ret;
}

private mixin template MongoUpdate(size_t idx, UPDATES...) {
	static if (UPDATES.length > 1) {
		mixin MongoUpdate!(idx, UPDATES[0 .. $/2]);
		mixin MongoUpdate!(idx + UPDATES.length/2, UPDATES[$/2 .. $]);
	} else static if (UPDATES.length == 1) {
		alias Q = UPDATES[0];

		static if (isInstanceOf!(SetExpr, Q)) {
			mixin(format(q{static struct Q%s { Q.ValueType %s; } @(vibe.data.serialization.name("$set")) Q%s q%s;}, idx, Q.fieldName, idx, idx));
		} else static assert(false, "Unsupported update expression type: "~Q.stringof);
	}
}

private mixin template MongoUpdates(size_t idx, UPDATES...) {
	static if (UPDATES.length > 1) {
		mixin MongoUpdates!(idx, UPDATES[0 .. $/2]);
		mixin MongoUpdates!(idx + UPDATES.length/2, UPDATES[$/2 .. $]);
	} else static if (UPDATES.length == 1) {
		mixin(format(q{struct Q%s { mixin MongoUpdate!(0, UPDATES[0]); } Q%s q%s;}, idx, idx, idx));
	}
}

private static string initializeMongoUpdate(size_t idx, UPDATE)(string name, string srcfield)
{
	string ret;
	alias Q = UPDATE;

	static if (isInstanceOf!(SetExpr, Q)) {
		ret ~= format("%s.q%s.%s = %s.value;", name, idx, Q.fieldName, srcfield);
	} else static assert(false, "Unsupported update expression type: "~Q.stringof);

	return ret;
}

