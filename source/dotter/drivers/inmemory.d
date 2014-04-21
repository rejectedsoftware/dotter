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
class InMemoryORMDriver {
	alias DefaultID = size_t; // running index
	alias TableHandle = size_t; // table index
	alias ColumnHandle = size_t; // byte offset
	enum bool supportsArrays = true;
	enum bool supportsJoins = false;

	private {
		static struct Table {
			string name;
			size_t[size_t] rowIndices;
			size_t rowCounter;
			ubyte[] storage;
			size_t idCounter;
		}
		Table[] m_tables;
	}

	size_t getTableHandle(T)(string name)
	{
		foreach (i, ref t; m_tables)
			if (t.name == name)
				return i;
		//import vibe.core.log; logInfo("table %s %s", name, m_tables.length);
		m_tables ~= Table(name);
		return m_tables.length - 1;
	}

	auto find(T, QUERY, TABLES...)(QUERY query, ref size_t[TABLES.length] table_handles)
	{
		/*import vibe.core.log;
		logInfo("tables before query:");
		foreach (i, t; m_tables)
			logInfo("%s: %s %s", i, t.storage.length, t.rowCounter);*/
		return MatchRange!(false, T, QUERY, TABLES)(this, query, table_handles);
	}

	void update(T, QUERY, UPDATE, TABLES...)(size_t table, QUERY query, UPDATE update, ref size_t[TABLES.length] table_handles)
	{
		auto ptable = &m_tables[table];
		auto items = cast(T[])ptable.storage;
		items = items[0 .. ptable.rowCounter];
		foreach (ref itm; MatchRange!(true, T, QUERY, TABLES)(this, query, table_handles))
			applyUpdate(itm, update);
	}

	void insert(T)(size_t table, T value)
	{
		import std.algorithm : max;
		auto ptable = &m_tables[table];
		if (ptable.storage.length <= ptable.rowCounter)
			ptable.storage.length = max(16 * T.sizeof, ptable.storage.length * 2);
		auto items = cast(T[])ptable.storage;
		items[ptable.rowCounter++] = value;
	}

	void updateOrInsert(T, QUERY)(size_t table, QUERY query, T value)
	{
		assert(false);
	}

	void removeAll(size_t table)
	{
		m_tables[table].rowCounter = 0;
	}

	private static void applyUpdate(T, U)(ref T item, ref U query)
	{
		static if (isInstanceOf!(SetExpr, U)) {
			__traits(getMember, item, U.name) = query.value;
		} else static assert(false, "Unsupported update expression type: "~U.stringof);
	}

	private ref inout(T) getItem(T)(size_t table, size_t item_index)
	inout {
		assert(table < m_tables.length, "Table index out of bounds.");
		auto items = cast(inout(T)[])m_tables[table].storage;
		import std.conv;
		assert(item_index < items.length, "Item index out of bounds for "~T.Table.stringof~" ("~to!string(table)~"): "~to!string(item_index));
		return items[item_index];
	}
}

private struct MatchRange(bool allow_modfications, T, QUERY, TABLES...)
{
	alias Tables = TABLES;
	enum iterationTables = QueryTables!(T.Table, QUERY);
	enum iterationTableIndex = tableIndicesOf!Tables(iterationTables);
	alias IterationTableTypes = IndexedTypes!(iterationTableIndex, Tables);
	//pragma(msg, "QUERY: "~QUERY.stringof);
	//pragma(msg, "ITTABLES: "~IterationTables.stringof);
	enum resultTableIndex = IndexOf!(T.Table, Tables);
	enum resultIterationTableIndex = iterationTables.countUntil(T.Table.stringof~".");

	private {
		InMemoryORMDriver m_driver;
		QUERY m_query;
		size_t[iterationTables.length] m_cursor;
		bool m_empty = false;
		size_t[Tables.length] m_handles;
	}

	this(InMemoryORMDriver driver, QUERY query, ref size_t[Tables.length] table_handles)
	{
		m_driver = driver;
		m_query = query;
		m_cursor[] = 0;
		m_handles = table_handles;
		findNextMatch();
	}

	@property bool empty() const { return m_empty; }

	static if (allow_modfications) {
		@property ref inout(T) front()
		inout {
			return m_driver.getItem!T(m_handles[resultTableIndex], m_cursor[resultIterationTableIndex]);
		}
	} else {
		@property ref const(T) front()
		const {
			return m_driver.getItem!T(m_handles[resultTableIndex], m_cursor[resultIterationTableIndex]);
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
			RawRows!(InMemoryORMDriver, IterationTableTypes) values;
			foreach (i, T; IterationTableTypes)
				values[i] = m_driver.getItem!(RawRow!(InMemoryORMDriver, T))(m_handles[iterationTableIndex[i]], m_cursor[i]);
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
			if (++idx >= m_driver.m_tables[m_handles[iterationTableIndex[i]]].rowCounter) idx = 0;
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
			alias valuerow = rows[iterationTables.countUntil(Q.valueTableName)];
			enum string cname = Q.valueColumnName;
			//pragma(msg, typeof(valuerow).stringof~" OO "~cname ~ " -> "~iterationTables.stringof~" -> "~IterationTableTypes.stringof~" -> "~iterationTableIndex.stringof);
			auto value = __traits(getMember, valuerow, cname);
		}
		static if (Q.op == CompareOp.equal) return __traits(getMember, item, Q.name) == value;
		else static if (Q.op == CompareOp.notEqual) return __traits(getMember, item, Q.name) != value;
		else static if (Q.op == CompareOp.greater) return __traits(getMember, item, Q.name) > value;
		else static if (Q.op == CompareOp.greaterEqual) return __traits(getMember, item, Q.name) >= value;
		else static if (Q.op == CompareOp.less) return __traits(getMember, item, Q.name) < value;
		else static if (Q.op == CompareOp.lessEqual) return __traits(getMember, item, Q.name) <= value;
		else static if (Q.op == CompareOp.contains) return __traits(getMember, item, Q.name).canFind(value);
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
}

size_t[] tableIndicesOf(TABLES...)(string[] names)
{
	import std.array : startsWith;
	auto ret = new size_t[names.length];
	foreach (i, T; TABLES)
		foreach (j, name; names)
			if (name.startsWith(T.stringof~"."))
				ret[j] = i;
	return ret;
}

template IndexedTypes(alias indices, T...)
{
	static if (indices.length == 0) alias IndexedTypes = TypeTuple!();
	else alias IndexedTypes = TypeTuple!(T[indices[0]], IndexedTypes!(indices[1 .. $], T));
}