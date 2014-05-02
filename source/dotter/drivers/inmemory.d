/**
	Memory based mapping driver.

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.drivers.inmemory;

import dotter.orm;

import std.algorithm : countUntil;
import std.traits;
import std.typetuple;


/** Simple in-memory ORM back end.

	This database back end is mostly useful as a lightweight replacement for
	a full database engine. It offers no data persistence across program runs.

	The primary uses for this class are to serve as a reference implementation
	and to enable unit testing without involving an external database process
	or disk access. However, it can also be useful in cases where persistence
	isn't needed, but where the ORM interface is already used.
*/
class InMemoryORMDriver(TABLES) {
	alias Tables = TABLES;
	alias DefaultID = size_t; // running index
	alias TableTypes = TypeTuple!(typeof(Tables.tupleof));
	enum bool supportsArrays = true;

	private {
		static struct Table {
			string name;
			size_t[size_t] rowIndices;
			size_t rowCounter;
			ubyte[] storage;
			size_t idCounter;
		}
		Table[TableTypes.length] m_tables;
	}

	this()
	{
		foreach (i, tname; __traits(allMembers, TABLES)) {
			m_tables[i] = Table(tname);
		}
	}

	auto find(T, QUERY)(QUERY query)
	{
		/*import vibe.core.log;
		logInfo("tables before query:");
		foreach (i, t; m_tables)
			logInfo("%s: %s %s", i, t.storage.length, t.rowCounter);*/
		return MatchRange!(false, T, QUERY, typeof(this))(this, query);
	}

	void update(T, QUERY, UPDATES...)(QUERY query, UPDATES updates)
	{
		auto ptable = &m_tables[staticIndexOf!(T, TableTypes)];
		auto items = cast(MemoryRow!(InMemoryORMDriver, T)[])ptable.storage;
		items = items[0 .. ptable.rowCounter];
		foreach (ref itm; MatchRange!(true, T, QUERY, typeof(this))(this, query))
			foreach (i, U; UPDATES)
				applyUpdate(itm, updates[i]);
	}

	void insert(T)(RawRow!(InMemoryORMDriver, T) value)
	{
		addRawItem(value);
	}

	void updateOrInsert(T, QUERY)(QUERY query, RawRow!(InMemoryORMDriver, T) value)
	{
		assert(false);
	}

	void remove(T, QUERY)(QUERY query)
	{
		assert(false);
	}

	void removeAll(T)()
	{
		m_tables[staticIndexOf!(T, TableTypes)].rowCounter = 0;
	}

	private void applyUpdate(T, U)(ref T item, ref U query)
	{
		static if (isInstanceOf!(SetExpr, U)) {
			__traits(getMember, item, U.fieldName) = query.value;
		} else static if (isInstanceOf!(PushExpr, U)) {
			foreach (v; query.values) {
				auto idx = addRawItem(v);
				__traits(getMember, item, U.fieldName) ~= idx;
			}
		} else static if (isInstanceOf!(PullExpr, U)) {
			foreach (v; query.values) {
				//auto idx = addRawItem(v);
				assert(false);
			}
		} else static assert(false, "Unsupported update expression type: "~U.stringof);
	}

	private size_t addRawItem(T)(RawRow!(InMemoryORMDriver, T) value)
	{
		import std.algorithm : max;
		alias MRow = MemoryRow!(InMemoryORMDriver, T);
		auto ptable = &m_tables[staticIndexOf!(T, TableTypes)];
		if (ptable.storage.length / MRow.sizeof <= ptable.rowCounter)
			ptable.storage.length = max(16 * MRow.sizeof, ptable.storage.length * 2);
		auto idx = ptable.rowCounter++;
		auto mval = toMemoryRow(value);
		auto items = cast(MRow[])ptable.storage;
		items[idx] = toMemoryRow(value);
		return idx;
	}

	private ref inout(MemoryRow!(InMemoryORMDriver, T)) getMemoryItem(T)(size_t table, size_t item_index)
	inout {
		assert(table < m_tables.length, "Table index out of bounds.");
		auto items = cast(inout(MemoryRow!(InMemoryORMDriver, T))[])m_tables[table].storage;
		import std.conv;
		assert(item_index < items.length, "Item index out of bounds for "~T.stringof~" ("~to!string(table)~"): "~to!string(item_index));
		return items[item_index];
	}

	private RawRow!(InMemoryORMDriver, T) getItem(T)(size_t table, size_t item_index)
	const {
		auto mrow = getMemoryItem!T(table, item_index);

		RawRow!(InMemoryORMDriver, T) ret;
		foreach (m; __traits(allMembers, T)) {
			alias M = typeof(__traits(getMember, T, m));
			static if (isTableDefinition!M && isOwned!M) {
				__traits(getMember, ret, m) = getItem(staticIndexOf!(M, TableTypes), __traits(getMember, mrow, m));
			} else static if (isArray!M && isTableDefinition!(typeof(M.init[0])) && isOwned!(typeof(M.init[0]))) {
				__traits(getMember, ret, m).length = __traits(getMember, mrow, m).length;
				foreach (i, ref dst; __traits(getMember, ret, m))
					dst = getItem!(typeof(M.init[0]))(staticIndexOf!(M, TableTypes), __traits(getMember, mrow, m)[i]);
			} else static if (isDynamicArray!M && !isSomeString!M) {
				__traits(getMember, ret, m) = __traits(getMember, mrow, m).dup;
			} else {
				__traits(getMember, ret, m) = __traits(getMember, mrow, m);
			}
		}
		return ret;
	}

	private MemoryRow!(InMemoryORMDriver, T) toMemoryRow(T)(RawRow!(InMemoryORMDriver, T) row)
	{
		MemoryRow!(InMemoryORMDriver, T) ret;
		foreach (f; __traits(allMembers, T)) {
			alias FT = typeof(__traits(getMember, T, f));
			static if (isTableDefinition!FT && isOwned!FT) {
				auto idx = addRawItem(__traits(getMember, row, f));
				__traits(getMember, ret, f) = idx;
			} else static if (isDynamicArray!FT && isTableDefinition!(typeof(FT.init[0])) && isOwned!(typeof(FT.init[0]))) {
				foreach (itm; __traits(getMember, row, f)) {
					auto idx = addRawItem(itm);
					__traits(getMember, ret, f) ~= idx;
				}
			} else {
				__traits(getMember, ret, f) = __traits(getMember, row, f);
			}
		}
		return ret;
	}
}

unittest {
	import dotter.test;
	static auto createDriver(TABLES)() { return new InMemoryORMDriver!(TABLES)(); }
	testDriver!(createDriver);
}

private struct MatchRange(bool allow_modfications, T, QUERY, DRIVER)
{
	alias Tables = DRIVER.TableTypes;
	enum iterationTables = QueryTables!(T, QUERY);
	enum iterationTableIndex = tableIndicesOf!Tables(iterationTables);
	alias IterationTableTypes = IndexedTypes!(iterationTableIndex, Tables);
	//pragma(msg, "QUERY: "~QUERY.stringof);
	//pragma(msg, "ITTABLES: "~IterationTables.stringof);
	enum resultTableIndex = IndexOf!(T, Tables);
	enum resultIterationTableIndex = iterationTables.countUntil(T.stringof~".");

	private {
		DRIVER m_driver;
		QUERY m_query;
		size_t[iterationTables.length] m_cursor;
		bool m_empty = false;
	}

	this(DRIVER driver, QUERY query)
	{
		m_driver = driver;
		m_query = query;
		m_cursor[] = 0;
		findNextMatch();
	}

	@property bool empty() const { return m_empty; }

	static if (allow_modfications) {
		@property ref inout(MemoryRow!(DRIVER, T)) front()
		inout {
			return m_driver.getMemoryItem!T(resultTableIndex, m_cursor[resultIterationTableIndex]);
		}
	} else {
		@property RawRow!(DRIVER, T) front()
		const {
			return m_driver.getItem!T(resultTableIndex, m_cursor[resultIterationTableIndex]);
		}
	}

	void popFront()
	{
		increment(true);
		findNextMatch();
	}

	private void findNextMatch()
	{
		while (!empty) {
			// 
			MemoryRows!(DRIVER, IterationTableTypes) values;
			foreach (i, T; IterationTableTypes) {
				// if any affected table is empty, there cannot be a result
				if (!m_driver.m_tables[iterationTableIndex[i]].rowCounter) {
					m_empty = true;
					break;
				}
				values[i] = m_driver.getMemoryItem!T(iterationTableIndex[i], m_cursor[i]);
			}
			//import std.stdio; writefln("TESTING %s %s", m_cursor, iterationTables);
			//static if (values.length == 4) writefln("%s %s %s %s", values[0], values[1], values[2], values[3]);
			if (matches(m_query, values)) break;
			increment(false);
		}
	}

	private void increment(bool next_result)
	{
		assert(!m_empty);
		if (m_empty) return;
		size_t first_table = next_result ? 1 : iterationTables.length;
		m_cursor[first_table .. $] = 0;
		foreach_reverse (i, ref idx; m_cursor[0 .. first_table]) {
			if (++idx >= m_driver.m_tables[iterationTableIndex[i]].rowCounter) idx = 0;
			else return;
		}
		m_empty = true;
	}

	private bool matches(Q, ROWS...)(ref Q query, ref ROWS rows)
		if (isInstanceOf!(CompareExpr, Q))
	{
		import std.algorithm : canFind;
		enum ri = iterationTables.countUntil(query.tableName);
		alias item = rows[ri];
		static if (is(typeof(Q.value))) {
			auto value = query.value;
		} else {
			//pragma(msg, "T "~Q.valueTableName~" C "~Q.valueColumnName);
			enum itidx = iterationTables.countUntil(Q.valueTableName);
			static if (isOwned!(Q.ValueTableType)) {
				auto value = m_cursor[itidx];
			} else {
				pragma(msg, "Not owned: "~Q.ValueTableType.stringof);
				enum string cname = Q.valueColumnName;
				alias valuerow = rows[itidx];
				//pragma(msg, typeof(valuerow).stringof~" OO "~cname ~ " -> "~iterationTables.stringof~" -> "~IterationTableTypes.stringof~" -> "~iterationTableIndex.stringof);
				auto value = __traits(getMember, valuerow, cname);
			}
		}
		static if (Q.op == CompareOp.equal) return __traits(getMember, item, Q.name) == value;
		else static if (Q.op == CompareOp.notEqual) return __traits(getMember, item, Q.name) != value;
		else static if (Q.op == CompareOp.greater) return __traits(getMember, item, Q.name) > value;
		else static if (Q.op == CompareOp.greaterEqual) return __traits(getMember, item, Q.name) >= value;
		else static if (Q.op == CompareOp.less) return __traits(getMember, item, Q.name) < value;
		else static if (Q.op == CompareOp.lessEqual) return __traits(getMember, item, Q.name) <= value;
		else static if (Q.op == CompareOp.contains) return __traits(getMember, item, Q.name).canFind(value);
		else static if (Q.op == CompareOp.anyOf) return value.canFind(__traits(getMember, item, Q.name));
		else static assert(false, format("Unsupported comparator: %s", Q.op));
	}

	private bool matches(Q, ROWS...)(ref Q query, ref ROWS rows)
		if (isInstanceOf!(ConjunctionExpr, Q))
	{
		foreach (i, E; typeof(Q.exprs))
			if (!matches(query.exprs[i], rows))
				return false;
		return true;
	}

	private bool matches(Q, ROWS...)(ref Q query, ref ROWS rows)
		if (isInstanceOf!(DisjunctionExpr, Q))
	{
		foreach (i, E; typeof(Q.exprs))
			if (matches(query.exprs[i], rows))
				return true;
		return false;
	}

	private bool matches(Q, ROWS...)(ref Q query, ref ROWS rows)
		if (is(Q == QueryAnyExpr))
	{
		return true;
	}
}

size_t[] tableIndicesOf(TABLES...)(string[] names)
{
	import std.array : startsWith;
	auto ret = new size_t[names.length];
	ret[] = size_t.max;
	foreach (i, T; TABLES)
		foreach (j, name; names)
			if (name.startsWith(T.stringof~"."))
				ret[j] = i;
	foreach (i, idx; ret) assert(idx != size_t.max, "Unknown table name ("~TABLES.stringof~"): "~names[i]);
	return ret;
}

template IndexedTypes(alias indices, T...)
{
	static if (indices.length == 0) alias IndexedTypes = TypeTuple!();
	else alias IndexedTypes = TypeTuple!(T[indices[0]], IndexedTypes!(indices[1 .. $], T));
}

struct MemoryRow(DRIVER, TABLE)
	if (isTableDefinition!TABLE)
{
	alias Table = TABLE;
	mixin MemoryRowFields!(DRIVER, TABLE, __traits(allMembers, TABLE));
}

mixin template MemoryRowFields(DRIVER, TABLE, MEMBERS...) {
	static if (MEMBERS.length > 1) {
		mixin MemoryRowFields!(DRIVER, TABLE, MEMBERS[0 .. $/2]);
		mixin MemoryRowFields!(DRIVER, TABLE, MEMBERS[$/2 .. $]);
	} else static if (MEMBERS.length == 1) {
		alias T = typeof(__traits(getMember, TABLE, MEMBERS[0]));
		mixin(format(`MemoryColumnType!(DRIVER, T) %s;`, MEMBERS[0]));
	}
}

template MemoryColumnType(DRIVER, T)
{
	static if (isTableDefinition!T) { // TODO: support in-document storage of table types for 1 to n relations
		static if (isOwned!T) alias MemoryColumnType = size_t;
		else alias MemoryColumnType = PrimaryKeyType!T;
	} else static if (isDynamicArray!T && !isSomeString!T && !is(T == ubyte[])) {
		alias E = typeof(T.init[0]);
		static assert(isTableDefinition!E, format("Array %s.%s may only contain table references, not %s.", TABLE.stringof, MEMBERS[0], E.stringof));
		static if (!isTableDefinition!E) static assert(false);
		else static if (DRIVER.supportsArrays) {
			static if (isOwned!E) alias MemoryColumnType = size_t[];
			else alias MemoryColumnType = PrimaryKeyType!E[]; // TODO: avoid dyamic allocations!
		} else {
			static assert(false, "Arrays for column based databases are not yet supported.");
		}
	} else {
		static assert(!isAssociativeArray!T, "Associative arrays are not supported as column types. Please use a separate table instead.");
		alias MemoryColumnType = T;
	}
}

template MemoryRows(DRIVER, T...)
{
	static if (T.length == 1) alias MemoryRows = TypeTuple!(MemoryRow!(DRIVER, T[0]));
	else static if (T.length == 0) alias MemoryRows = TypeTuple!();
	else alias MemoryRows = TypeTuple!(MemoryRows!(DRIVER, T[0 .. $/2]), MemoryRows!(DRIVER, T[$/2 .. $]));
}

