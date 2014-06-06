/**
	Cassandra based mapping driver.

	Note that this module requires the vibe-d dependency to be present.

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.drivers.cassandra;

version (Have_cassandra_d):

import dotter.orm;

import cassandra.cassandradb;
import cassandra.cql.connection : PreparedStatement;
import cassandra.cql.result;
import cassandra.table;
import std.traits;
import std.typetuple;
import vibe.core.log;
import vibe.data.serialization;


/** ORM driver using MongoDB for data storage and query execution.

	The driver generates static types used to efficiently and directly
	serialize query expressions to BSON without unnecessary memory allocations.
*/
class CassandraDriver(TABLES) {
	alias Tables = TABLES;
	alias TableTypes = TypeTuple!(typeof(TABLES.tupleof));
	enum fieldTableNames = getFieldTableNames!TABLES();
	pragma(msg, fieldTableNames);

	private {
		CassandraKeyspace m_keyspace;
		CassandraTable[TableTypes.length] m_tables;
		CassandraTable[fieldTableNames.length] m_fieldTables; // tables used for storing collection fields
		PreparedStatement[string] m_preparedStatements;
	}

	alias DefaultID = int;
	alias TableHandle = CassandraTable;
	alias ColumnHandle = string;
	enum bool supportsArrays = true;

	this(CassandraKeyspace keyspace)
	{
		m_keyspace = keyspace;

		// create unowned tables
		foreach (i, tname; __traits(allMembers, TABLES)) {
			alias TABLE = typeof(__traits(getMember, TABLES, tname));
			static if (!isOwned!TABLE) {
				try m_tables[i] = m_keyspace.createTable!(CassandraRow!TABLE)(TABLE.stringof, primaryKey(primaryKeyOf!TABLE));
				catch (Exception) m_tables[i] = m_keyspace.getTable(TABLE.stringof);
				foreach (f; __traits(allMembers, TABLE))
					static if (isColumnIndexed!(TABLE, f))
						try m_tables[i].createIndex(f);
						catch (Exception e) logDiagnostic("Error creating index %s.%s: %s", TABLE.stringof, f, e.msg);
			}
		}

		// create collection tables
		size_t i = 0;
		foreach (T; typeof(TABLES.tupleof))
			foreach (f; __traits(allMembers, T)) {
				alias FT = typeof(__traits(getMember, T, f));
				static if (isDynamicArray!FT && isTableDefinition!(typeof(FT.init[0]))) {
					alias FTT = typeof(FT.init[0]);
					enum name = T.stringof ~ "_" ~ f;
					assert(name == fieldTableNames[i]);
					static if (isOwned!FTT) {
						try m_tables[i] = m_keyspace.createTable!(CassandraRow!(FTT, PrimaryKeyType!T))(name, primaryKey("owner"));
						catch (Exception) m_fieldTables[i] = m_keyspace.getTable(name);
					} else {
						import std.string : toLower;
						enum ownerName = T.stringof.toLower;
						enum owneeName = FTT.stringof.toLower;
						static struct Relation {
							/*@name(ownerName)*/ PrimaryKeyType!T owner;
							/*@name(owneeName)*/ PrimaryKeyType!FTT ownee;
						}
						try m_tables[i] = m_keyspace.createTable!Relation(name, primaryKey("owner"));
						catch (Exception) m_fieldTables[i] = m_keyspace.getTable(name);
					}
					i++;
				}
			}

		// TODO: create indices
	}

	this(string url_or_host, string keyspace_name)
	{
		auto cli = connectCassandraDB(url_or_host);
		CassandraKeyspace kspace;
		try kspace = cli.getKeyspace(keyspace_name);
		catch (Exception) kspace = cli.createKeyspace(keyspace_name);
		this(kspace);
	}
	
	auto find(T, QUERY)(QUERY query)
	{
		PreparedStatement stmt;
		if (auto ps = __FUNCTION__ in m_preparedStatements) stmt = *ps;
		else {
			auto stmtstr = queryToCQL!T(query);
			import vibe.core.log; logInfo("%s", stmtstr);
			stmt = m_keyspace.prepare(stmtstr);
			stmt.consistency = Consistency.one;
			m_preparedStatements[__FUNCTION__] = stmt;
		}

		logInfo("EXEC %s with %s params", queryToCQL!T(query), queryParameters(query).length);

		auto res = m_keyspace.execute(stmt, queryParameters(query).expand);
		return RowRange!(RawRow!T)(res);
	}

	void update(T, QUERY, UPDATE)(QUERY query, UPDATE update)
	{
		assert(false);
	}

	void insert(T)(T value)
	{
		coll!(T.Table).insert(toCassandraRow(value));
	}

	void updateOrInsert(T, QUERY)(QUERY query, T value)
	{
		assert(false);
	}

	void removeAll(TABLE)()
	{
		coll!TABLE.truncate();
	}

	private ref CassandraTable coll(TABLE)()
	{
		enum idx = staticIndexOf!(TABLE, TableTypes);
		static assert(idx >= 0, "Table "~TABLE.stringof~" doesn't exist.");
		return m_tables[idx];
	}
	private ref CassandraTable fieldColl(TABLE, string field)()
	{
		enum name = TABLE.stringof ~ "_" ~ field;
		enum idx = fieldTableNames.countUntil(name);
		static assert(idx >= 0, "Field table "~name~" doesn't exist.");
		return m_fieldTables[idx];
	}

	private RawRow!T getItem(T)(size_t table, size_t item_index)
	const {
		auto mrow = getCassandraItem!T(table, item_index);

		RawRow!T ret;
		foreach (m; __traits(allMembers, T)) {
			alias M = typeof(__traits(getMember, T, m));
			static if (isTableDefinition!M && isOwned!M) {
				__traits(getMember, ret, m) = getItem(staticIndexOf!(M, TableTypes), __traits(getMember, mrow, m));
			} else static if (isArray!M && isTableDefinition!(typeof(M.init[0])) && isOwned!(typeof(M.init[0]))) {
				alias MTable = typeof(M.init[0]);
				__traits(getMember, ret, m).length = __traits(getMember, mrow, m).length;
				foreach (i, ref dst; __traits(getMember, ret, m))
					dst = getItem!MTable(staticIndexOf!(MTable, TableTypes), __traits(getMember, mrow, m)[i]);
			} else static if (isDynamicArray!M && !isSomeString!M) {
				__traits(getMember, ret, m) = __traits(getMember, mrow, m).dup;
			} else {
				__traits(getMember, ret, m) = fromStoredType!(RawColumnType!M)(__traits(getMember, mrow, m));
			}
		}
		return ret;
	}

	private CassandraRow!(T, OWNER_KEY) toCassandraRow(T, OWNER_KEY...)(RawRow!T row, OWNER_KEY owner_key)
		if (OWNER_KEY.length <= 1)
	{
		CassandraRow!(T, OWNER_KEY) ret;
		static if (OWNER_KEY.length) ret.owner = owner_key[0];
		foreach (f; __traits(allMembers, T)) {
			alias FT = typeof(__traits(getMember, T, f));
			static if (isTableDefinition!FT) {
				static if (isOwned!FT) static assert(false, "Owned references not supported.");
				else __traits(getMember, ret, f) = __traits(getMember, row, f); // primary key
			} else static if (isDynamicArray!FT && isTableDefinition!(typeof(FT.init[0]))) {
				alias FET = typeof(FT.init[0]);
				static if (isOwned!FET) {
					foreach (itm; __traits(getMember, row, f))
						insertOwnedRow!(T, f, FET)(getPrimaryKey(row), itm);
				} else {
					foreach (itm; __traits(getMember, row, f))
						insertForeignRow!(T, f, FET)(getPrimaryKey(row), itm);
				}
			} else {
				__traits(getMember, ret, f) = toStoredType(__traits(getMember, row, f));
			}
		}
		return ret;
	}

	private void insertOwnedRow(T, string field, OT)(PrimaryKeyType!T pk, in ref RawRow!OT row)
	{
		static assert(is(OT == typeof(__traits(getMember, T, field).init[0])), "Unexpected owned table");
		fieldColl!(T, field).insert(toCassandraRow(row, pk));
	}

	private void insertForeignRow(T, string field, FT)(PrimaryKeyType!T pk, PrimaryKeyType!FT fk)
	{
		import std.string : toLower;
		static assert(is(FT == typeof(__traits(getMember, T, field).init[0])), "Unexpected foreign table");
		enum ownerName = T.stringof.toLower;
		enum owneeName = FT.stringof.toLower;
		static struct Relation {
			@name(ownerName) PrimaryKeyType!T owner;
			@name(owneeName) PrimaryKeyType!FT ownee;
		}
		Relation r;
		r.owner= pk;
		r.ownee = fk;
		fieldColl!(T, field).insert(r);
	}
}

private string queryToCQL(T, QUERY)(QUERY query)
{
	static if (is(QUERY == QueryAnyExpr)) {
		return "SELECT * FROM "~T.stringof;
	} else static if (isInstanceOf!(CompareExpr, QUERY)) {
		string op;
		final switch (QUERY.op) with (CompareOp) {
			case equal: op = "="; break;
			case notEqual: assert(false, "Cassandra has no != operator.");
			case greater: op = ">"; break;
			case greaterEqual: op = ">="; break;
			case less: op = "<"; break;
			case lessEqual: op = "<="; break;
			case contains:
			case anyOf:
				assert(false);
		}
		return "SELECT * FROM "~QUERY.TABLE.stringof~" WHERE "~QUERY.name ~ " "~op~" ? ALLOW FILTERING"; // TODO: handle field tables properly!
	} else {
		assert(false, "Unsupported query: "~QUERY.stringof);
	}
}

auto queryParameters(QUERY)(QUERY query)
{
	import std.typecons;
	static if (is(QUERY == QueryAnyExpr)) return Tuple!()();
	else static if (isInstanceOf!(CompareExpr, QUERY)) {
		return tuple(query.value);
	} else assert(false, "Unsupported query: "~QUERY.stringof);
}

unittest {
	import dotter.test;
	static auto createDriver(TABLES)(CassandraKeyspace db) { return new CassandraDriver!(TABLES)(db); }

	CassandraClient client;
	try client = connectCassandraDB("127.0.0.1");
	catch (Exception e) {
		import vibe.core.log;
		logWarn("Failed to connect to local Cassandra server. Skipping test.");
		return;
	}
	CassandraKeyspace db;
	try db = client.getKeyspace("test");
	catch (Exception) db = client.createKeyspace("test");
	testDriver!(createDriver)(db);
}


struct RowRange(T) {
	import std.typecons;

	private {
		struct Data {
			this(CassandraResult res) { result = res; }
			CassandraResult result;
			bool frontValid = false;
			T front;
		}
		alias DataRef = RefCounted!(Data, RefCountedAutoInitialize.no);
		DataRef m_data;
	}
	
	this(CassandraResult result)
	{
		m_data = DataRef(result);
	}

	bool empty() { return !m_data.frontValid && m_data.result.empty; }
	
	ref T front()
	{
		if (!m_data.frontValid) {
			import vibe.core.log; logInfo("reading row %s", T.stringof);
			m_data.result.readRow!T(m_data.front);
			logInfo("read row.");
			m_data.frontValid = true;
		}
		return m_data.front;
	}

	void popFront()
	{
		if (m_data.frontValid) m_data.frontValid = false;
		else m_data.result.dropRow();
	}
}

struct CassandraRow(TABLE, OWNER_KEY...)
	if (isTableDefinition!TABLE && OWNER_KEY.length <= 1)
{
	alias Table = TABLE;
	static if (OWNER_KEY.length) OWNER_KEY[0] owner;
	mixin CassandraRowFields!(TABLE, __traits(allMembers, TABLE));
}

mixin template CassandraRowFields(TABLE, MEMBERS...) {
	static if (MEMBERS.length > 1) {
		mixin CassandraRowFields!(TABLE, MEMBERS[0 .. $/2]);
		mixin CassandraRowFields!(TABLE, MEMBERS[$/2 .. $]);
	} else static if (MEMBERS.length == 1) {
		alias T = typeof(__traits(getMember, TABLE, MEMBERS[0]));
		static if (!is(CassandraColumnType!T == void))
			mixin(format(`CassandraColumnType!T %s;`, MEMBERS[0]));
	}
}

template CassandraColumnType(T)
{
	static if (isTableDefinition!T) {
		static if (isOwned!T) static assert(false, "Direct owned references not supported, insert field directly for now.");
		else alias CassandraColumnType = StoredType!(PrimaryKeyType!T);
	} else static if (isDynamicArray!T && isTableDefinition!(typeof(T.init[0]))) {
		alias E = typeof(T.init[0]);
		alias CassandraColumnType = void; // normalized storage
	} else {
		static assert(!isAssociativeArray!T, "Associative arrays are not supported as column types. Please use a separate table instead.");
		alias CassandraColumnType = StoredType!T;
	}
}

string[] getFieldTableNames(TABLES)()
{
	string[] ret;
	foreach (T; typeof(TABLES.tupleof))
		foreach (f; __traits(allMembers, T)) {
			alias FT = typeof(__traits(getMember, T, f));
			static if (isDynamicArray!FT && isTableDefinition!(typeof(FT.init[0]))) {
				enum name = T.stringof ~ "_" ~ f;
				ret ~= name;
			}
		}
	return ret;
}

auto getPrimaryKey(T)(RawRow!T row)
{
	enum pk = primaryKeyOf!T;
	return __traits(getMember, row, pk);
}