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
	db.insert!User(0, "Tom", 45);
	db.insert!User(1, "Peter", 13);
	db.insert!User(2, "Peter", 42);
	db.insert!User(3, "Foxy", 8);
	db.insert!User(4, "Peter", 69);

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
	db.insert!User("Tom");
	db.insert!User("Peter");
	db.insert!User("Foxy");
	db.insert!User("Lynn");
	db.insert!User("Hartmut");

	db.removeAll!Box();
	db.insert!Box("box 1", ["Tom", "Foxy"]);
	db.insert!Box("box 2", ["Tom", "Hartmut", "Lynn"]);
	db.insert!Box("box 3", ["Lynn", "Hartmut", "Peter"]);

	import std.stdio;
	import std.array;
	writefln("RES: %s", array(db.find(cmp!Box.users.contains("Hartmut") & cmp!Box.users.contains("Lynn")).map!(r => r.toTuple)));

	assert(db.find!Box(cmp!Box.users.contains("Hartmut") & cmp!Box.users.contains("Lynn")).map!(r => r.toTuple).equal([
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
	db.users.insert("Peter");
	db.users.insert("Tom");
	db.users.insert("Stacy");

	db.groups.removeAll();
	db.groups.insert("drivers");
	db.groups.insert("sellers");

	db.groupMembers.removeAll();
	db.groupMembers.insert("drivers", "Peter", "leader");
	db.groupMembers.insert("drivers", "Tom", "worker");
	db.groupMembers.insert("sellers", "Stacy", "leader");
	db.groupMembers.insert("sellers", "Peter", "staff");
	
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
	db.users.insert("Peter");
	db.users.insert("Tom");
	db.users.insert("Stacy");

	db.groups.removeAll();
	db.groups.insert("drivers");
	db.groups.insert("sellers");

	db.groupMembers.removeAll();
	db.groupMembers.insert("drivers", "Peter", "leader");
	db.groupMembers.insert("drivers", "Tom", "worker");
	db.groupMembers.insert("sellers", "Stacy", "leader");
	db.groupMembers.insert("sellers", "Peter", "staff");
	
	assert(db.find(GroupMember.name("Tom")).map!(r => r.toTuple).equal([
		tuple("drivers", "Tom", "worker")
	]));
	
	assert(db.find!GroupMember(GroupMember.group("sellers")).map!(r => r.toTuple).equal([
		tuple("sellers", "Stacy", "leader"),
		tuple("sellers", "Peter", "staff")
	]));
}*/


/// Advanced nested queries
unittest {
	import dotter.drivers.inmemory;
	import std.algorithm : equal;

	@tableDefinition
	struct User {
	static:
		@primaryKey string name;
	}

	@tableDefinition
	struct GroupMember {
	static:
		@primaryKey int dummy;
		//@owner Group group;
		User user;
		string role;
	}

	@tableDefinition
	struct Group {
	static:
		@primaryKey string name;
		@owned @unordered GroupMember[] members;
	}

	struct Tables {
		User users;
		Group groups;
		GroupMember groupMembers;
	}

	auto dbdriver = new InMemoryORMDriver;
	auto db = createORM!Tables(dbdriver);

	db.removeAll!User();
	db.insert!User("Peter");
	db.insert!User("Linda");
	db.insert!User("Tom");

	db.removeAll!GroupMember();
	db.insert!GroupMember(0, "Peter", "leader");
	db.insert!GroupMember(1, "Tom", "worker");
	db.insert!GroupMember(2, "Linda", "leader");
	db.insert!GroupMember(3, "Peter", "staff");

	db.removeAll!Group();
	db.insert!Group("drivers", [0, 1]);
	db.insert!Group("sellers", [2, 3]);

	// determine the groups that "linda" is in
	assert(db.find!Group(cmp!Group.members.contains(cmp!GroupMember) & cmp!GroupMember.user("Linda")).map!(r => r.toTuple()).equal([
		tuple("sellers", [2, 3])
	]));

	// determine all users in group "drivers"
	assert(db.find!User(cmp!Group.name("drivers") & cmp!Group.members.contains(cmp!GroupMember) & cmp!GroupMember.user(cmp!User.name)).map!(r => r.toTuple()).equal([
		tuple("Peter"),
		tuple("Tom")
	]));

	// find co-workers of Linda
	GroupMember m1, m2;
	import std.array;
	import vibe.core.log;
	logInfo("RESULT: %s", db.find!User(cmp!Group.members.contains(cmp!m1) & cmp!Group.members.contains(cmp!m2) & cmp!m1.user("Linda") & cmp!m2.user(cmp!User) & cmp!User.name.notEqual("Linda")).map!(r => r.toTuple()).array);
	assert(db.find!User(cmp!Group.members.contains(cmp!m1) & cmp!Group.members.contains(cmp!m2) & cmp!m1.user("Linda") & cmp!m2.user(cmp!User.name) & cmp!User.name.notEqual("Linda")).map!(r => r.toTuple()).equal([
		tuple("Peter")
	]));
	//db.find(cmp!User.groups.members.matchesAll!(GroupMember.user("Linda"), GroupMember.user(User.name)), cmp!User.name.notEqual("Linda"));
	/*assert(db.find!User(cmp!Group.members.matchesAll(cmp!GroupMember.user("Linda"), cmp!GroupMember.user(User.name)) & cmp!User.name.notEqual("Linda")).map!(r => r.toTuple()).equal([
		tuple("Peter")
	]));*/
}


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
	alias TableTypes = TypeTuple!(typeof(TABLES.tupleof));
	enum tableNames = [__traits(allMembers, TABLES)];

	private {
		Driver m_driver;
		Driver.TableHandle[TableTypes.length] m_tables;
	}

	this(Driver driver)
	{
		m_driver = driver;

		foreach (i, tname; __traits(allMembers, Tables)) {
			//pragma(msg, "TAB "~tname);
			alias Table = typeof(__traits(getMember, Tables, tname));
			static assert(isTableDefinition!Table, "Table defintion lacks @TableDefinition UDA: "~Table.stringof);
			m_tables[i] = driver.getTableHandle!Table(tname);
			foreach (f; __traits(allMembers, Table)) {
				alias FieldType = typeof(__traits(getMember, Table, f));
				static assert(isValidColumnType!FieldType, "Unsupported column type for "~tname~"."~f~": "~FieldType.stringof);
			}
		}

		upgradeColumns();
	}

	/// The underlying database driver
	@property inout(Driver) driver() inout { return m_driver; }

	/** Queries a table for a set of rows.

		The return value is an input range of type Row!(T, ORM), where T is the type
		of the underlying table.
	*/
	template find(FIELDS...)
	{
		auto find(QUERY)(QUERY query)
		{
			static if (FIELDS.length == 0) {
				alias T = QueryTable!QUERY;
				enum tidx = tableIndex!(T, Tables);
				return m_driver.find!(RawRow!(Driver, T), QUERY, TableTypes)(query, m_tables).map!(r => new Row!(ORM, T)(this, r));
			} else static if (FIELDS.length == 1 && isTableDefinition!(FIELDS[0])) {
				alias T = FIELDS[0];
				enum tidx = tableIndex!(T, Tables);
				return m_driver.find!(RawRow!(Driver, T), QUERY, TableTypes)(query, m_tables).map!(r => new Row!(ORM, T)(this, r));
			} else {
				static assert(false, "Selecting individual fields is not yet supported.");
			}
		}
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
		return m_driver.findRaw!(RawRow!(Driver, TABLE))(m_tables[tidx], params);
	}

	void update(QUERY, UPDATE)(QUERY query, UPDATE update)
	{
		alias T = QueryTable!QUERY;
		auto tidx = tableIndex!(T, Tables);
		m_driver.update!(RawRow!(Driver, T), QUERY, UPDATE, TableTypes)(m_tables[tidx], query, update, m_tables);
	}

	void insert(T, FIELDS...)(FIELDS fields)
		if (isTableDefinition!T)
	{
		enum tidx = tableIndex!(T, Tables);
		RawRow!(Driver, T) value;
		// TODO: translate references to other tables automatically
		value.tupleof = fields;
		m_driver.insert(m_tables[tidx], value);
	}

	/*void updateOrInsert(QUERY query)(QUERY query, QueryTable)

	void remove(QUERY query)
	{

	}*/

	void removeAll(T)()
		if (isTableDefinition!T)
	{
		enum tidx = tableIndex!(T, Tables);
		m_driver.removeAll(m_tables[tidx]);
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

CMP!TABLE cmp(TABLE...)() { return CMP!TABLE.init; }
CMPColumn!(COLUMN, TABLE, TABLE_NAME) cmpcolumn(string COLUMN, TABLE, string TABLE_NAME)() { return CMPColumn!(COLUMN, TABLE, TABLE_NAME).init; }

struct CMP(TABLE...)
	if (TABLE.length == 1 && (is(TABLE) && isTableDefinition!TABLE || isTableDefinition!(typeof(TABLE))))
{
	static if (is(TABLE)) {
		alias TableType = TABLE[0];
		enum tableName = TableType.stringof ~ ".";
	} else {
		alias TableType = typeof(TABLE[0]);
		enum tableName = TableType.stringof ~ "." ~ __traits(identifier, TABLE[0]);
	}

	private mixin template CmpFields(MEMBERS...) {
		static if (MEMBERS.length > 1) {
			mixin CmpFields!(MEMBERS[0 .. $/2]);
			mixin CmpFields!(MEMBERS[$/2 .. $]);
		} else static if (MEMBERS.length == 1) {
			//alias T = typeof(__traits(getMember, Table, MEMBERS[0]));
			//pragma(msg, format(`enum %s = cmpcolumn!("%s", TableType, tableName);`, MEMBERS[0], MEMBERS[0]));
			mixin(format(`enum %s = cmpcolumn!("%s", TableType, tableName);`, MEMBERS[0], MEMBERS[0]));
		}
	}

	mixin CmpFields!(__traits(allMembers, TableType));
}

struct CMPColumn(string COLUMN, TABLE, string TABLE_NAME)
	if (isTableDefinition!TABLE)
{
	alias TableType = TABLE;
	enum tableName = TABLE_NAME;
	enum columnName = COLUMN;

	alias Field = typeof(__traits(getMember, TableType, columnName));
	alias FieldComparator = ComparatorType!Field;

	static opCall(OP)(OP operand) if(isOperand!(OP, Field)) { return equal(operand); }
	static auto equal(OP)(OP operand) if(isOperand!(OP, Field)) { return compare!(CompareOp.equal)(operand); }
	static auto notEqual(OP)(OP operand) if(isOperand!(OP, Field)) { return compare!(CompareOp.notEqual)(operand); }
	static auto greater(OP)(OP operand) if(isOperand!(OP, Field)) { return compare!(CompareOp.greater)(operand); }
	static auto greaterEqual(OP)(OP operand) if(isOperand!(OP, Field)) { return compare!(CompareOp.greaterEqual)(operand); }
	static auto less(OP)(OP operand) if(isOperand!(OP, Field)) { return compare!(CompareOp.less)(operand); }
	static auto lessEqual(OP)(OP operand) if(isOperand!(OP, Field)) { return compare!(CompareOp.lessEqual)(operand); }

	static auto compare(CompareOp comp, OP)(OP operand)
		if(isOperand!(OP, Field))
	{
		CompareExpr!(tableName, __traits(getMember, TableType, columnName), comp, OP) ret;
		static if (is(OP == FieldComparator)) ret.value = operand;
		return ret;
	}

	static if (isArray!Field && isTableDefinition!(typeof(Field.init[0]))) {
		//pragma(msg, Field);
		alias Table = typeof(Field.init[0]);
		static auto contains(OP)(OP operand) if(isOperand!(OP, Field)) { return compare!(CompareOp.contains)(operand); }
	}
}

@property auto and(EXPRS...)(EXPRS exprs) { return ConjunctionExpr!EXPRS(exprs); }
@property auto or(EXPRS...)(EXPRS exprs) { return DisjunctionExpr!EXPRS(exprs); }
//JoinExpr!()

struct CompareExpr(string TABLE_NAME, alias FIELD, CompareOp OP, MATCH...)
	if (MATCH.length == 1)
{
	alias TABLE = TypeTuple!(__traits(parent, FIELD))[0];
	enum tableName = TABLE_NAME;
	alias T = typeof(FIELD);
	alias V = ComparatorType!T;
	enum name = __traits(identifier, FIELD);
	enum op = OP;

	static if (isInstanceOf!(CMPColumn, MATCH[0])) {
		//pragma(msg, "CMPCOLUMN! "~MatchType.tableName~" "~MatchType.columnName);
		alias MatchType = MATCH[0];
		alias ValueTableType = MatchType.TableType;
		enum valueTableName = MatchType.tableName;
		enum valueColumnName = MatchType.columnName;
	} else static if (isInstanceOf!(CMP, MATCH[0])) {
		alias MatchType = MATCH[0];
		alias ValueTableType = MatchType.TableType;
		enum valueTableName = MatchType.tableName;
		enum valueColumnName = primaryKeyOf!ValueTableType;
	} else {
		//pragma(msg, "T: "~MATCH[0].stringof);
		V value;
	}

	auto opBinaryRight(string op, T)(T other) if(op == "|") { return DisjunctionExpr!(T, typeof(this))(other, this); }
	auto opBinaryRight(string op, T)(T other) if(op == "&") { return ConjunctionExpr!(T, typeof(this))(other, this); }
	//auto opBinaryRight(string op, T)(T other) if (op == "|" && isInstanceOf)
}
enum CompareOp {
	equal,
	notEqual,
	greater,
	greaterEqual,
	less,
	lessEqual,
	contains
}

template ComparatorType(T)
{
	static if (isTableDefinition!T) alias ComparatorType = PrimaryKeyType!T;
	else static if (isArray!T && isTableDefinition!(typeof(T.init[0]))) alias ComparatorType = PrimaryKeyType!(typeof(T.init[0]));
	else static if (!isArray!T || isSomeString!T) alias ComparatorType = T;
	else static assert(false, "Cannot query non-table array type columns ("~T.stringof~").");
}
struct ConjunctionExpr(EXPRS...) { EXPRS exprs; }
struct DisjunctionExpr(EXPRS...) { EXPRS exprs; }

private template isOperand(T, FIELD)
{
	alias FieldComparator = ComparatorType!FIELD;
	static if (is(T == FieldComparator)) enum isOperand = true;
	else static if (isInstanceOf!(CMPColumn, T) && is(T.FieldComparator == FieldComparator)) enum isOperand = true;
	else static if (isInstanceOf!(CMP, T) && is(PrimaryKeyType!(T.TableType) == FieldComparator)) enum isOperand = true;
	else enum isOperand = false;
}


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

template isValidColumnType(T) {
	static if (isTableDefinition!T) enum isValidColumnType = true;
	else static if (isDynamicArray!T && isTableDefinition!(typeof(T.init[0]))) enum isValidColumnType = true;
	else static if (is(T == bool)) enum isValidColumnType = true;
	else static if (is(T == ubyte) || is(T == byte)) enum isValidColumnType = true;
	else static if (is(T == ushort) || is(T == short)) enum isValidColumnType = true;
	else static if (is(T == uint) || is(T == int)) enum isValidColumnType = true;
	else static if (is(T == ulong) || is(T == long)) enum isValidColumnType = true;
	else static if (is(T == float) || is(T == double)) enum isValidColumnType = true;
	else static if (is(T == string)) enum isValidColumnType = true;
	else enum isValidColumnType = false;
}

class Row(ORM, TABLE)
	if (isTableDefinition!TABLE)
{
	alias Driver = ORM.Driver;

	private {
		ORM m_orm;
		const(RawRow!(ORM.Driver, TABLE)) m_rawData;
	}

	this(ORM orm, in RawRow!(ORM.Driver, TABLE) data)
	{
		m_orm = orm;
		m_rawData = data;
	}

	@property ref const(RawRow!(ORM.Driver, TABLE)) rawRowData() const { return m_rawData; }

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
		} else static if (isDynamicArray!T && !isSomeString!T && !is(T == ubyte[])) {
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
		const(E)[] m_items;
	}

	this(ORM orm, in E[] items)
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
		return m_orm.findOne(__traits(getMember, CMP!T, primaryKeyName)(key));
	}
}


struct RawRow(DRIVER, TABLE)
	if (isTableDefinition!TABLE)
{
	alias Table = TABLE;
	mixin RawRowFields!(DRIVER, TABLE, __traits(allMembers, TABLE));
}

mixin template RawRowFields(DRIVER, TABLE, MEMBERS...) {
	static if (MEMBERS.length > 1) {
		mixin RawRowFields!(DRIVER, TABLE, MEMBERS[0 .. $/2]);
		mixin RawRowFields!(DRIVER, TABLE, MEMBERS[$/2 .. $]);
	} else static if (MEMBERS.length == 1) {
		alias T = typeof(__traits(getMember, TABLE, MEMBERS[0]));
		mixin(format(`RawColumnType!(DRIVER, T) %s;`, MEMBERS[0]));
	}
}

template RawColumnType(DRIVER, T)
{
	static if (isTableDefinition!T) { // TODO: support in-document storage of table types for 1 to n relations
		alias RawColumnType = PrimaryKeyType!T;
	} else static if (isDynamicArray!T && !isSomeString!T && !is(T == ubyte[])) {
		alias E = typeof(T.init[0]);
		static assert(isTableDefinition!E, format("Array %s.%s may only contain table references, not %s.", TABLE.stringof, MEMBERS[0], E.stringof));
		static if (!isTableDefinition!E) static assert(false);
		else static if (DRIVER.supportsArrays) {
			alias RawColumnType = PrimaryKeyType!E[]; // TODO: avoid dyamic allocations!
		} else {
			static assert(false, "Arrays for column based databases are not yet supported.");
		}
	} else {
		static assert(!isAssociativeArray!T, "Associative arrays are not supported as column types. Please use a separate table instead.");
		alias RawColumnType = T;
	}
}

template RawRows(DRIVER, T...)
{
	static if (T.length == 1) alias RawRows = TypeTuple!(RawRow!(DRIVER, T[0]));
	else static if (T.length == 0) alias RawRows = TypeTuple!();
	else alias RawRows = TypeTuple!(RawRows!(DRIVER, T[0 .. $/2]), RawRows!(DRIVER, T[$/2 .. $]));
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
		} else static if (isInstanceOf!(CompareExpr, Q) || isInstanceOf!(MatchExpr, Q)) {
			alias QueryTable = Q.TABLE;
		} else static assert(false, "Invalid query type: "~Q.stringof);
	} else {
		alias T1 = QueryTable!(QUERIES[0 .. $/2]);
		alias T2 = QueryTable!(QUERIES[$/2 .. $]);
		static assert(is(T1 == T2), "Query references different tables: "~T1.stringof~" and "~T2.stringof);
		alias QueryTable = T1;
	}
}

template QueryTables(QUERIES...) if (QUERIES.length > 0) {
	static if (QUERIES.length == 1) {
		alias Q = QUERIES[0];
		static if (isInstanceOf!(ConjunctionExpr, Q) || isInstanceOf!(DisjunctionExpr, Q)) {
			enum QueryTables = QueryTables!(typeof(Q.exprs));
		} else static if (isInstanceOf!(CompareExpr, Q)/* || isInstanceOf!(MatchExpr, Q)*/) {
			enum QueryTables = [Q.tableName];
		} else static if (isTableDefinition!Q) {
			enum QueryTables = [Q.stringof~"."];
		} else static assert(false, "Invalid query type: "~Q.stringof);
	} else static if (QUERIES.length > 1) {
		import std.algorithm : canFind;
		enum head = QueryTables!(QUERIES[0]);
		enum remainder = QueryTables!(QUERIES[1 .. $]);
		static if (remainder.canFind(head)) enum QueryTables = remainder;
		else enum QueryTables = head ~ remainder;
	} else enum string[] QueryTables = [];
}

private template tableIndex(TABLE, TABLES)
{
	template impl(size_t idx, MEMBERS...) {
		static assert(idx < MEMBERS.length, "Invalid table: "~TABLE.stringof);
		enum member = MEMBERS[idx];
		static if (is(typeof(__traits(getMember, TABLES, member)) == TABLE)) enum impl = idx;
		else enum impl = impl!(idx+1, MEMBERS);
	}
	enum tableIndex = impl!(0, __traits(allMembers, TABLES));
}
