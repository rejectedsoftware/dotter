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
import vibe.data.serialization;


/** ORM driver using MongoDB for data storage and query execution.

	The driver generates static types used to efficiently and directly
	serialize query expressions to BSON without unnecessary memory allocations.
*/
class MongoDBDriver {
	import vibe.db.mongo.mongo;

	private {
		MongoDatabase m_db;
	}

	alias DefaultID = BsonObjectID;
	alias TableHandle = MongoCollection;
	alias ColumnHandle = string;
	enum bool supportsArrays = true;
	enum bool supportsJoins = false;

	this(string url_or_host, string name)
	{
		auto cli = connectMongoDB(url_or_host);
		m_db = cli.getDatabase(name);
	}

	MongoCollection getTableHandle(T)(string name)
	{
		// TODO: setup keys, especially the primary key!
		return m_db[name];
	}
	
	auto find(T, QUERY)(MongoCollection table, QUERY query)
	{
		struct Query { mixin MongoQuery!(0, QUERY); }
		Query mquery;
		mixin(initializeMongoQuery!(0, QUERY)("mquery", "query"));
		
		import vibe.core.log; import vibe.data.bson;
		//logInfo("QUERY (%s): %s", table.name, serializeToBson(mquery).toString());
		
		return table.find(mquery).map!(b => deserializeBson!T(b));
	}

	void update(T, QUERY, UPDATE)(MongoCollection table, QUERY query, UPDATE update)
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

		table.update(mquery, mupdate);
	}

	void insert(T)(MongoCollection table, T value)
	{
		table.insert(value);
	}

	void updateOrInsert(T, QUERY)(size_t table, QUERY query, T value)
	{
		assert(false);
	}

	void removeAll(MongoCollection table)
	{
		table.remove(Bson.emptyObject);
	}
}

private mixin template MongoQuery(size_t idx, QUERIES...) {
	static if (QUERIES.length > 1) {
		mixin MongoQuery!(idx, QUERIES[0 .. $/2]);
		mixin MongoQuery!(idx + QUERIES.length/2, QUERIES[$/2 .. $]);
	} else static if (QUERIES.length == 1) {
		static assert(!is(typeof(QUERIES[0])) || is(typeof(QUERIES[0])), "Arguments to MongoQuery must be types.");
		alias Q = QUERIES[0];

		static if (isInstanceOf!(ComparatorExpr, Q)) {
			static if (Q.comp == Comparator.equal) mixin("Q.V "~Q.name~";");
			else static if (Q.comp == Comparator.notEqual) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$ne")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.comp == Comparator.greater) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$gt")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.comp == Comparator.greaterEqual) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$gte")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.comp == Comparator.less) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$lt")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.comp == Comparator.lessEqual) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$lte")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static if (Q.comp == Comparator.containsAll) mixin(format(`static struct Q%s { @(vibe.data.serialization.name("$all")) Q.V value; } Q%s %s;`, idx, idx, Q.name));
			else static assert(false, format("Unsupported comparator: %s", Q.comp));
		} else static if (isInstanceOf!(ConjunctionExpr, Q)) {
			//mixin(format(`static struct Q%s { mixin MongoQuery!(0, Q.exprs); } @(vibe.data.serialization.name("$and")) Q%s q%s;`, idx, idx, idx));
			mixin MongoQuery!(0, typeof(Q.exprs));
		} else static if (isInstanceOf!(DisjunctionExpr, Q)) {
			mixin(format(
				q{static struct Q%s { mixin MongoQueries!(0, typeof(Q.exprs)); } @(vibe.data.serialization.name("$or"), asArray) Q%s q%s;}, idx, idx, idx));
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

	static if (isInstanceOf!(ComparatorExpr, Q)) {
		final switch (Q.comp) with (Comparator) {
			case equal:
				ret ~= format("%s.%s = %s.value;", name, Q.name, srcfield);
				break;
			case notEqual, greater, greaterEqual, less, lessEqual:
			case containsAll:
				ret ~= format("%s.%s.value = %s.value;", name, Q.name, srcfield);
				break;
		}
	} else static if (isInstanceOf!(ConjunctionExpr, Q)) {
		foreach (i, E; typeof(Q.exprs))
			//ret ~= initializeMongoQuery!(i, E)(format("%s.q%s", name, idx), format("%s.exprs[%s]", srcfield, i));
			ret ~= initializeMongoQuery!(i, E)(name, format("%s.exprs[%s]", srcfield, i));
	} else static if (isInstanceOf!(DisjunctionExpr, Q)) {
		foreach (i, E; typeof(Q.exprs))
			ret ~= initializeMongoQuery!(i, E)(format("%s.q%s.q%s", name, idx, i), format("%s.exprs[%s]", srcfield, i));
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
			mixin(format(q{static struct Q%s { Q.T %s; } @(vibe.data.serialization.name("$set")) Q%s q%s;}, idx, Q.name, idx, idx));
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
		ret ~= format("%s.q%s.%s = %s.value;", name, idx, Q.name, srcfield);
	} else static assert(false, "Unsupported update expression type: "~Q.stringof);

	return ret;
}

