/**
	Database independent object relational mapping framework.

	This framework adds a typesafe layer on top of a database driver for
	creating, modifying and querying the data. It aims to use no dynamic memory
	allocations wherever possible.

	Open_issues:
		How to deal with schema changes?

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.orm;

import dotter.internal.uda;

import std.algorithm : map;
import std.string : format;
import std.traits;
import std.typetuple;
import std.typecons : Nullable, tuple;


/// Simple example of defining tables and inserting/querying/updating rows.
unittest {
	import dotter.drivers.inmemory;
	import std.algorithm : equal;

	@tableDefinition
	struct User {
		static:
		@primaryKey int id;
		string name;
		int age;
	}

	struct Tables {
		User users;
	}

	//auto dbdriver = new MongoDBDriver("127.0.0.1", "test");
	auto dbdriver = new InMemoryORMDriver;

	auto db = createORM!Tables(dbdriver);
	db.removeAll!User();
	db.insertRow!User(0, "Tom", 45);
	db.insertRow!User(1, "Peter", 13);
	db.insertRow!User(2, "Peter", 42);
	db.insertRow!User(3, "Foxy", 8);
	db.insertRow!User(4, "Peter", 69);

	assert(db.find(and(cmp!User.name("Peter"), cmp!User.age.greater(29))).map!(r => r.toTuple).equal([
		tuple(2, "Peter", 42),
		tuple(4, "Peter", 69)
	]));

	assert(db.find(cmp!User.name("Peter") & cmp!User.age.greater(29)).map!(r => r.toTuple).equal([
		tuple(2, "Peter", 42),
		tuple(4, "Peter", 69)
	]));

	assert(db.find(or(cmp!User.name("Peter"), cmp!User.age.greater(29))).map!(r => r.toTuple).equal([
		tuple(0, "Tom", 45),
		tuple(1, "Peter", 13),
		tuple(2, "Peter", 42),
		tuple(4, "Peter", 69)
	]));

	db.update(cmp!User.name("Tom"), set!(User.age)(20));

	assert(db.find(cmp!User.name("Tom")).map!(r => r.toTuple).equal([
		tuple(0, "Tom", 20)
	]));
}


/// Connecting tables using collections
unittest {
	import dotter.drivers.inmemory;
	import std.algorithm : equal;

	@tableDefinition
	struct User {
		static:
		@primaryKey
		string name;
	}

	@tableDefinition
	struct Box {
		static:
		@primaryKey
		string name;
		User[] users;
	}

	struct Tables {
		User users;
		Box boxes;
	}

	//auto dbdriver = new MongoDBDriver("127.0.0.1", "test");
	auto dbdriver = new InMemoryORMDriver;
	auto db = createORM!Tables(dbdriver);

	db.removeAll!User();
	db.insertRow!User("Tom");
	db.insertRow!User("Peter");
	db.insertRow!User("Foxy");
	db.insertRow!User("Lynn");
	db.insertRow!User("Hartmut");

	db.removeAll!Box();
	db.insertRow!Box("box 1", ["Tom", "Foxy"]);
	db.insertRow!Box("box 2", ["Tom", "Hartmut", "Lynn"]);
	db.insertRow!Box("box 3", ["Lynn", "Hartmut", "Peter"]);

	assert(db.find(cmp!Box.users.containsAll("Hartmut", "Lynn")).map!(r => r.toTuple).equal([
		tuple("box 2", ["Tom", "Hartmut", "Lynn"]),
		tuple("box 3", ["Lynn", "Hartmut", "Peter"])
	]));
}

/// Using owned tables for more efficient storage/processing
/*unittest {
	import dotter.drivers.inmemory;
	import std.algorithm : equal;

	@tableDefinition
	struct User {
	static:
		@primaryKey string name;
	}

	@tableDefinition
	struct Group {
	static:
		@primaryKey string name;
		@owned @unordered GroupMember[] members;
	}

	@tableDefinition
	static struct GroupMember {
	static:
		@owner Group group;
		User user;
		string type;
	}

	struct Tables {
		User users;
		Group groups;
		GroupMember groupMembers;
	}

	auto dbdriver = new InMemoryORMDriver;
	auto db = new ORM!Tables(db);

	db.users.removeAll();
	db.users.insertRow("Peter");
	db.users.insertRow("Tom");
	db.users.insertRow("Stacy");

	db.groups.removeAll();
	db.groups.insertRow("drivers");
	db.groups.insertRow("sellers");

	db.groupMembers.removeAll();
	db.groupMembers.insertRow("drivers", "Peter", "leader");
	db.groupMembers.insertRow("drivers", "Tom", "worker");
	db.groupMembers.insertRow("sellers", "Stacy", "leader");
	db.groupMembers.insertRow("sellers", "Peter", "staff");
	
	assert(db.find!GroupMember(GroupMember.group("sellers")).map!(r => r.toTuple).equal([
		tuple("sellers", "Stacy", "leader"),
		tuple("sellers", "Peter", "staff")
	]));
}

// Using co-ownership to extend the possibility for efficient queries (may cause some denormalization)
unittest {
	import dotter.drivers.inmemory;
	import std.algorithm : equal;

	@tableDefinition
	struct User {
	static:
		@primaryKey string name;
		@coOwned @unordered GroupMember[] memberships;
	}

	@tableDefinition
	struct Group {
	static:
		@primaryKey string name;
		@coOwned @unordered GroupMember[] members;
	}

	@tableDefinition @ownedBy!Group
	struct GroupMember {
	static:
		@owner Group group;
		@owner User user;
		string type;
	}

	struct Tables {
		User users;
		Group groups;
		GroupMember groupMembers;
	}

	auto dbdriver = new InMemoryORMDriver;
	auto db = new ORM!Tables(db);

	db.users.removeAll();
	db.users.insertRow("Peter");
	db.users.insertRow("Tom");
	db.users.insertRow("Stacy");

	db.groups.removeAll();
	db.groups.insertRow("drivers");
	db.groups.insertRow("sellers");

	db.groupMembers.removeAll();
	db.groupMembers.insertRow("drivers", "Peter", "leader");
	db.groupMembers.insertRow("drivers", "Tom", "worker");
	db.groupMembers.insertRow("sellers", "Stacy", "leader");
	db.groupMembers.insertRow("sellers", "Peter", "staff");
	
	assert(db.find(GroupMember.name("Tom")).map!(r => r.toTuple).equal([
		tuple("drivers", "Tom", "worker")
	]));
	
	assert(db.find!GroupMember(GroupMember.group("sellers")).map!(r => r.toTuple).equal([
		tuple("sellers", "Stacy", "leader"),
		tuple("sellers", "Peter", "staff")
	]));
}*/


// just playing with ideas for query syntaxes
auto dummy = q{
	// the current solution. works, but kind of ugly
	auto res = m_db.find(and(.equal!(User.name)("Peter"), greater!(User.age)(min_age)));
	// short, but what to do with explicit joins?
	auto res = m_db.find!User(equal!"name"("Peter") & greater!"age"(min_age));
	// short, but what to do with explicit joins? and requires a parser
	auto res = m_db.find!(User, q{name == "Peter" && age > args[0]})(min_age);
	// clean syntax, but needs a parser and mixins in user code are kind of ugly
	auto res = mixin(find("m_db", q{User.name == "Peter" && User.age > min_age}));
	// using expression templates where possible, simple extension to the current solution, puts the comparison operator in the middle
	auto res = m_db.find(Cmp!(User.name, "==")("Peter") & Cmp!(User.age, ">")(min_age));
	auto res = m_db.find(Cmp!(User.name)("Peter") & Cmp!(User.age, ">")(min_age));
	auto res = m_db.find(Cmp!User.name.equal("Peter") & Cmp!User.age.greater(min_age));
	auto res = m_db.find(Cmp!User.name("Peter") & Cmp!User.age!">"(min_age));
	auto res = m_db.find(Cmp!User.name("Peter") & Cmp!User.age(greater(min_age)));
	// requires different way to define the tables
	auto res = m_db.find(User.name.equal("Peter") & User.age.greater(min_age));
	auto res = m_db.find(User.name.cmp!"=="("Peter") & User.age.cmp!">"(min_age));
	auto res = m_db.find(User.name("Peter") & User.age!">"(min_age));
	auto res = m_db.find(User.name("Peter") & User.age(greater(min_age)));
	// short for complex expressions, but long for simple ones
	auto res = m_db.find!((Var!User u) => u.name.equal("Peter") & u.age.greater(min_age));
};


/** Required attribute to mark a struct as a table definition.
*/
@property TableDefinitionAttribute tableDefinition() { return TableDefinitionAttribute.init; }

/** Marks the primary key of a table.

	Only one column per table may be the primary key. Whenever columns
	of the type of the table are compared to other columns/values,
	the primary key is used for this comparison.
*/
@property PrimaryKeyAttribute primaryKey() { return PrimaryKeyAttribute.init; }

/** Notes a secondary key of a table.
*/

/** Marks a column as containing the referenced table rows.

	This assumption will be enforced in conjunction with the $(D @ownedBy!T)
	attribute and by disallowing defining an explicit primary key.
*/
@property OwnedAttribute owned() { return OwnedAttribute.init; }

/** Sets the owner table for an owned table.

	This attribute must be present on any table referenced using the
	$(D @owned) attribute. It enforces that only the owner table
	can have references to the owned table and that there is a strict
	one to many relationship between owner and ownee.
*/
@property OwnerByAttribute owner() { return OwnerByAttribute.init; }

/** Hints the database that the order of array elements does not matter.

	Array fields with this attribute can be freely reordered by the
	database for more efficient access.
*/
@property UnorderedAttribute unordered() { return UnorderedAttribute.init; }

struct TableDefinitionAttribute {}
struct PrimaryKeyAttribute {}
struct OwnedAttribute {}
struct OwnerByAttribute {}
struct UnorderedAttribute {}


ORM!(Tables, Driver) createORM(Tables, Driver)(Driver driver) { return new ORM!(Tables, Driver)(driver); }

class ORM(TABLES, DRIVER) {
	alias Tables = TABLES;
	alias Driver = DRIVER;

	private {
		Driver m_driver;
		struct TableInfo {
			Driver.TableHandle handle;
			//Driver.ColumnHandle[] columnHandles;
		}
		TableInfo[] m_tables;
	}

	this(Driver driver)
	{
		m_driver = driver;

		foreach (tname; __traits(allMembers, Tables)) {
			//pragma(msg, "TAB "~tname);
			alias Table = typeof(__traits(getMember, Tables, tname));
			static assert(isTableDefinition!Table, "Table defintion lacks @TableDefinition UDA: "~Table.stringof);
			TableInfo ti;
			ti.handle = driver.getTableHandle!Table(tname);
			foreach (cname; __traits(allMembers, Table)) {
				//pragma(msg, "COL "~cname);
				//ti.columnHandles ~= driver.getColumnHandle(ti.handle, cname);
			}
			m_tables ~= ti;
		}

		upgradeColumns();
	}

	/// The underlying database driver
	@property inout(Driver) driver() inout { return m_driver; }

	/** Queries a table for a set of rows.

		The return value is an input range of type Row!(T, ORM), where T is the type
		of the underlying table.
	*/
	auto find(QUERY)(QUERY query)
	{
		alias T = QueryTable!QUERY;
		enum tidx = tableIndex!(T, Tables);
		return m_driver.find!(RawRow!(ORM, T))(m_tables[tidx].handle, query).map!(r => Row!(ORM, T)(this, r));
	}

	/** Queries a table for the first match.

		A Nullable!T is returned and set to null when no match was found.
	*/
	Nullable!(Row!(ORM, QueryTable!QUERY)) findOne(QUERY)(QUERY query)
	{
		auto res = find(query); // TODO: give a hint to the DB driver that only one document is desired
		Nullable!(Row!(ORM, QueryTable!QUERY)) ret;
		if (!res.empty) ret = res.front;
		return ret;
	}

	/** Driver specific version of find.

		This method takes a set of driver defined arguments (e.g. a
		string plus parameters for an SQL database or a BSON document
		for the MongoDB driver).
	*/
	auto findRaw(TABLE, T...)(T params)
	{
		enum tidx = tableIndex!(T, Tables);
		return m_driver.findRaw!(RawRow!TABLE)(m_tables[tidx].handle, params);
	}

	void update(QUERY, UPDATE)(QUERY query, UPDATE update)
	{
		alias T = QueryTable!QUERY;
		auto tidx = tableIndex!(T, Tables);
		m_driver.update!(RawRow!(ORM, T))(m_tables[tidx].handle, query, update);
	}

	void insertRow(T, FIELDS...)(FIELDS fields)
		if (isTableDefinition!T)
	{
		enum tidx = tableIndex!(T, Tables);
		RawRow!(ORM, T) value;
		// TODO: translate references to other tables automatically
		value.tupleof = fields;
		m_driver.insert(m_tables[tidx].handle, value);
	}

	/*void updateOrInsert(QUERY query)(QUERY query, QueryTable)

	void remove(QUERY query)
	{

	}*/

	void removeAll(T)()
		if (isTableDefinition!T)
	{
		enum tidx = tableIndex!(T, Tables);
		m_driver.removeAll(m_tables[tidx].handle);
	}

	/*void insert(T)(Item!T item)
	{
		m_driver.insert(item);
	}

	void remove!(QUERY...)()
		if (QUERY.length == 1)
	{
		return m_driver.remove(QUERY);
	}*/

	private void upgradeColumns()
	{

	}
}


/*struct Column(T) {
	alias Type = T;

	auto equal()
}*/

/******************************************************************************/
/* QUERY EXPRESSIONS                                                          */
/******************************************************************************/

struct cmp(TABLE)
	if (isTableDefinition!TABLE)
{
	static struct cmpfield(string column)
	{
		alias FIELD = ComparatorType!(typeof(__traits(getMember, TABLE, column)));
		static opCall(FIELD value) { return equal(value); }
		static auto equal(FIELD value) { return compare!(Comparator.equal)(value); }
		static auto notEqual(FIELD value) { return compare!(Comparator.notEqual)(value); }
		static auto greater(FIELD value) { return compare!(Comparator.greater)(value); }
		static auto greaterEqual(FIELD value) { return compare!(Comparator.greaterEqual)(value); }
		static auto less(FIELD value) { return compare!(Comparator.less)(value); }
		static auto lessEqual(FIELD value) { return compare!(Comparator.lessEqual)(value); }
		static if (isArray!FIELD) {
			// FIXME: avoid dynamic array here:
			static auto containsAll(FIELD values...) { return compare!(Comparator.containsAll)(values); }
		}
		static auto compare(Comparator comp)(FIELD value) { return ComparatorExpr!(__traits(getMember, TABLE, column), comp)(value); }
	}

	mixin template CmpFields(MEMBERS...) {
		static if (MEMBERS.length > 1) {
			mixin CmpFields!(MEMBERS[0 .. $/2]);
			mixin CmpFields!(MEMBERS[$/2 .. $]);
		} else static if (MEMBERS.length == 1) {
			alias T = typeof(__traits(getMember, TABLE, MEMBERS[0]));
			//pragma(msg, "MEMBER: "~MEMBERS[0]);
			mixin(format(`alias %s = cmpfield!"%s";`, MEMBERS[0], MEMBERS[0]));
		}
	}

	mixin CmpFields!(__traits(allMembers, TABLE));
}

@property auto and(EXPRS...)(EXPRS exprs) { return ConjunctionExpr!EXPRS(exprs); }
@property auto or(EXPRS...)(EXPRS exprs) { return DisjunctionExpr!EXPRS(exprs); }
//JoinExpr!()

struct ComparatorExpr(alias FIELD, Comparator COMP)
{
	alias T = typeof(FIELD);
	alias V = ComparatorType!T;
	alias TABLE = TypeTuple!(__traits(parent, FIELD))[0];
	enum name = __traits(identifier, FIELD);
	enum comp = COMP;
	V value;

	auto opBinary(string op, T)(T other) if(op == "|") { return DisjunctionExpr!(typeof(this), T)(this, other); }
	auto opBinary(string op, T)(T other) if(op == "&") { return ConjunctionExpr!(typeof(this), T)(this, other); }
}
enum Comparator {
	equal,
	notEqual,
	greater,
	greaterEqual,
	less,
	lessEqual,
	containsAll
}
template ComparatorType(T)
{
	static if (isTableDefinition!T) alias ComparatorType = PrimaryKeyType!T;
	else static if (isArray!T && isTableDefinition!(typeof(T.init[0]))) alias ComparatorType = PrimaryKeyType!(typeof(T.init[0]))[];
	else alias ComparatorType = T;
}
struct ConjunctionExpr(EXPRS...) { EXPRS exprs; }
struct DisjunctionExpr(EXPRS...) { EXPRS exprs; }


/******************************************************************************/
/* UPDATE EXPRESSIONS                                                         */
/******************************************************************************/

auto set(alias field)(typeof(field) value) { return SetExpr!(field)(value); }

struct SetExpr(alias FIELD)
{
	alias T = typeof(FIELD);
	alias TABLE = TypeTuple!(__traits(parent, FIELD))[0];
	enum name = __traits(identifier, FIELD);
	T value;
}


/******************************************************************************/
/* UTILITY TEMPLATES                                                          */
/******************************************************************************/

struct Row(ORM, TABLE)
	if (isTableDefinition!TABLE)
{
	private {
		ORM m_orm;
		RawRow!(ORM, TABLE) m_rawData;
	}

	this(ORM orm, RawRow!(ORM, TABLE) data)
	{
		m_orm = orm;
		m_rawData = data;
	}

	@property ref const(RawRow!(ORM, TABLE)) rawRowData() const { return m_rawData; }

	auto toTuple() { return tuple(m_rawData.tupleof); }

	mixin RowFields!(ORM, TABLE, __traits(allMembers, TABLE));
}

mixin template RowFields(ORM, TABLE, MEMBERS...) {
	static if (MEMBERS.length > 1) {
		mixin RowFields!(ORM, TABLE, MEMBERS[0 .. $/2]);
		mixin RowFields!(ORM, TABLE, MEMBERS[$/2 .. $]);
	} else static if (MEMBERS.length == 1) {
		alias T = typeof(__traits(getMember, TABLE, MEMBERS[0]));
		static if (isTableDefinition!T) {
			mixin(format(`@property auto %s() { return m_orm.findOne(cmp!T.%s(m_rawData.%s)); }`, MEMBERS[0], primaryKeyOf!T, MEMBERS[0]));
		} else static if (isDynamicArray!T && !isSomeString!T) {
			alias E = typeof(T.init[0]);
			static assert(isTableDefinition!E);
			static if (!isTableDefinition!E) static assert(false);
			static if (ORM.Driver.supportsArrays) {
				mixin(format(`@property auto %s() { return RowArray!(ORM, E)(m_orm, m_rawData.%s); }`, MEMBERS[0], MEMBERS[0]));
			} else {
				static assert(false);
			}
		} else {
			static assert(!isAssociativeArray!T);
			mixin(format(`@property auto %s() const { return m_rawData.%s; }`, MEMBERS[0], MEMBERS[0]));
		}
	}
}

struct RowArray(ORM, T) {
	private {
		alias E = PrimaryKeyType!T;
		alias R = Row!(ORM, T);
		enum primaryKeyName = primaryKeyOf!T;
		ORM m_orm;
		E[] m_items;
	}

	this(ORM orm, E[] items)
	{
		m_orm = orm;
		m_items = items;
	}

	R opIndex(size_t idx) { return resolve(m_items[idx]); }

	auto opSlice()
	{
		static int dummy; dummy++; // force method to be impure to work around DMD 2.065 issue
		return m_items.map!(itm => resolve(itm));
	}

	private R resolve(E key)
	{
		return m_orm.findOne(__traits(getMember, cmp!T, primaryKeyName)(key));
	}
}


struct RawRow(ORM, TABLE)
	if (isTableDefinition!TABLE)
{
	mixin RawRowFields!(ORM, TABLE, __traits(allMembers, TABLE));
}

mixin template RawRowFields(ORM, TABLE, MEMBERS...) {
	static if (MEMBERS.length > 1) {
		mixin RawRowFields!(ORM, TABLE, MEMBERS[0 .. $/2]);
		mixin RawRowFields!(ORM, TABLE, MEMBERS[$/2 .. $]);
	} else static if (MEMBERS.length == 1) {
		alias T = typeof(__traits(getMember, TABLE, MEMBERS[0]));
		mixin(format(`RawColumnType!(ORM, T) %s;`, MEMBERS[0]));
	}
}

template RawColumnType(ORM, T)
{
	static if (isTableDefinition!T) { // TODO: support in-document storage of table types for 1 to n relations
		alias RawColumnType = PrimaryKeyType!T;
	} else static if (isDynamicArray!T && !isSomeString!T) {
		alias E = typeof(T.init[0]);
		static assert(isTableDefinition!E, format("Array %s.%s may only contain table references, not %s.", TABLE.stringof, MEMBERS[0], E.stringof));
		static if (!isTableDefinition!E) static assert(false);
		else static if (ORM.Driver.supportsArrays) {
			alias RawColumnType = PrimaryKeyType!E[]; // TODO: avoid dyamic allocations!
		} else {
			static assert(false, "Arrays for column based databases are not yet supported.");
		}
	} else {
		static assert(!isAssociativeArray!T, "Associative arrays are not supported as column types. Please use a separate table instead.");
		alias RawColumnType = T;
	}
}

template isTableDefinition(T) {
	static if (is(T == struct)) enum isTableDefinition = findFirstUDA!(TableDefinitionAttribute, T).found;
	else enum isTableDefinition = false;
}
template isPrimaryKey(T, string key) { enum isPrimaryKey = findFirstUDA!(PrimaryKeyAttribute, __traits(getMember, T, key)).found; }

@property string primaryKeyOf(T)()
	if (isTableDefinition!T)
{
	// TODO: produce better error messages for duplicate or missing primary keys!
	foreach (m; __traits(allMembers, T))
		static if (isPrimaryKey!(T, m))
			return m;
	assert(false, "No primary key for "~T.stringof);
}

template PrimaryKeyType(T) if (isTableDefinition!T) { enum string key = primaryKeyOf!T; alias PrimaryKeyType = typeof(__traits(getMember, T, key)); }


private template QueryTable(QUERIES...) if (QUERIES.length > 0) {
	static if (QUERIES.length == 1) {
		alias Q = QUERIES[0];
		static if (isInstanceOf!(ConjunctionExpr, Q) || isInstanceOf!(DisjunctionExpr, Q)) {
			alias QueryTable = QueryTable!(typeof(Q.exprs));
		} else static if (isInstanceOf!(ComparatorExpr, Q) || isInstanceOf!(ContainsExpr, Q)) {
			alias QueryTable = Q.TABLE;
		} else static assert(false, "Invalid query type: "~Q.stringof);
	} else {
		alias T1 = QueryTable!(QUERIES[0 .. $/2]);
		alias T2 = QueryTable!(QUERIES[$/2 .. $]);
		static assert(is(T1 == T2), "Query references different tables: "~T1.stringof~" and "~T2.stringof);
		alias QueryTable = T1;
	}
}

private template tableIndex(TABLE, TABLES)
{
	template impl(size_t idx, MEMBERS...) {
		static if (MEMBERS.length > 1) {
			enum a = impl!(0, MEMBERS[0 .. $/2]);
			enum b = impl!(MEMBERS.length/2, MEMBERS[$/2 .. $]);
			enum impl = a != size_t.max ? a : b;
		} else static if (MEMBERS.length == 1) {
			enum mname = MEMBERS[0];
			static if (is(typeof(__traits(getMember, TABLES, mname)) == TABLE))
				enum impl = idx;
			else enum impl = size_t.max;
		} else enum impl = size_t.max;
	}
	enum tableIndex = impl!(0, __traits(allMembers, TABLES));
	static assert(tableIndex != size_t.max, "Invalid table: "~TABLE.stringof);
}
