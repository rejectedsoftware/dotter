/**
	Memory based mapping driver.

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.drivers.inmemory;

import dotter.orm;

import std.traits;


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
		m_tables ~= Table(name);
		return m_tables.length - 1;
	}

	auto find(T, QUERY)(size_t table, QUERY query)
	{
		import std.algorithm : filter;
		auto ptable = &m_tables[table];
		auto items = cast(T[])ptable.storage;
		items = items[0 .. ptable.rowCounter];
		return filter!((itm => matchQuery(itm, query)))(items);
	}

	void update(T, QUERY, UPDATE)(size_t table, QUERY query, UPDATE update)
	{
		auto ptable = &m_tables[table];
		auto items = cast(T[])ptable.storage;
		items = items[0 .. ptable.rowCounter];
		foreach (ref itm; items)
			if (matchQuery(itm, query))
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

	private static bool matchQuery(T, Q)(ref T item, ref Q query)
	{
		static if (isInstanceOf!(ComparatorExpr, Q)) {
			static if (Q.comp == Comparator.equal) return __traits(getMember, item, Q.name) == query.value;
			else static if (Q.comp == Comparator.notEqual) return __traits(getMember, item, Q.name) != query.value;
			else static if (Q.comp == Comparator.greater) return __traits(getMember, item, Q.name) > query.value;
			else static if (Q.comp == Comparator.greaterEqual) return __traits(getMember, item, Q.name) >= query.value;
			else static if (Q.comp == Comparator.less) return __traits(getMember, item, Q.name) < query.value;
			else static if (Q.comp == Comparator.lessEqual) return __traits(getMember, item, Q.name) <= query.value;
			else static if (Q.comp == Comparator.containsAll) {
				import std.algorithm : canFind;
				foreach (v; query.value)
					if (!canFind(__traits(getMember, item, Q.name), v))
						return false;
				return true;
			} else static assert(false, format("Unsupported comparator: %s", Q.comp));
		} else static if (isInstanceOf!(ConjunctionExpr, Q)) {
			foreach (i, E; typeof(Q.exprs))
				if (!matchQuery(item, query.exprs[i]))
					return false;
			return true;
		} else static if (isInstanceOf!(DisjunctionExpr, Q)) {
			foreach (i, E; typeof(Q.exprs))
				if (matchQuery(item, query.exprs[i]))
					return true;
			return false;
		} else static assert(false, "Unsupported query expression type: "~Q.stringof);
	}

	private static void applyUpdate(T, U)(ref T item, ref U query)
	{
		static if (isInstanceOf!(SetExpr, U)) {
			__traits(getMember, item, U.name) = query.value;
		} else static assert(false, "Unsupported update expression type: "~U.stringof);
	}
}
