/**
	Conformance tests for driver implementations.

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.test;

import dotter.orm;

import std.algorithm : equal, map;


void testDriver(alias CREATE_DRIVER, CREATE_PARAMS...)(CREATE_PARAMS create_params)
{
	auto drv = CREATE_DRIVER!Tables(create_params);
	auto db = createORM(drv);

	db.users.insert(0, "Dummy", 0, null);
	db.users.removeAll();
	assert(db.users.find().map!(u => u.toTuple()).empty);

	db.users.insert(0, "Peter", 21, [1, 2]);
	db.users.insert(1, "Stacy", 33, [3]);
	db.users.insert(2, "Tom", 45, [3, 4]);
	db.users.insert(3, "Linda", 28, null);
	db.users.insert(4, "Jack", 19, [3]);
	db.users.insert(5, "Russel", 52, [3, 4]);
	db.users.insert(6, "Hans", 64, null);
	db.users.insert(7, "Peter", 42, [6]);

	assert(db.users.find().map!(u => u.toTuple()).setEqual([
		tuple(0, "Peter", 21, [1, 2]),
		tuple(1, "Stacy", 33, [3]),
		tuple(2, "Tom", 45, [3, 4]),
		tuple(3, "Linda", 28, cast(int[])[]),
		tuple(4, "Jack", 19, [3]),
		tuple(5, "Russel", 52, [3, 4]),
		tuple(6, "Hans", 64, cast(int[])[]),
		tuple(7, "Peter", 42, [6])
	]));

	//static assert(!__traits(compiles, db.groupMembers.removeAll()));

	db.groups.removeAll();
	assert(db.groups.find().map!(g => g.toTuple()).empty);

	db.groups.insert("computer", [
		RawRow!GroupMember(1, "admin"),
		RawRow!GroupMember(0, "member"),
		RawRow!GroupMember(2, "member"),
		RawRow!GroupMember(4, "member"),
	]);
	db.groups.insert("stitching", [
		RawRow!GroupMember(1, "member"),
		RawRow!GroupMember(3, "admin"),
		RawRow!GroupMember(4, "member"),
		RawRow!GroupMember(6, "member"),
	]);
	db.groups.insert("cooking", [
		RawRow!GroupMember(3, "admin"),
		RawRow!GroupMember(4, "member"),
		RawRow!GroupMember(6, "member"),
		RawRow!GroupMember(7, "member"),
	]);

	assert(db.groups.find().map!(g => g.toTuple()).setEqual([
		tuple("computer", [
			RawRow!GroupMember(1, "admin"),
			RawRow!GroupMember(0, "member"),
			RawRow!GroupMember(2, "member"),
			RawRow!GroupMember(4, "member"),
		]),
		tuple("stitching", [
			RawRow!GroupMember(1, "member"),
			RawRow!GroupMember(3, "admin"),
			RawRow!GroupMember(4, "member"),
			RawRow!GroupMember(6, "member"),
		]),
		tuple("cooking", [
			RawRow!GroupMember(3, "admin"),
			RawRow!GroupMember(4, "member"),
			RawRow!GroupMember(6, "member"),
			RawRow!GroupMember(7, "member"),
		]),
	]));

	//
	// basic queries
	//
	assert(db.find(var!User.name("Peter")).map!(u => u.id).setEqual([0, 7]));
	assert(db.find(var!User.name.equal("Peter")).map!(u => u.id).setEqual([0, 7]));
	assert(db.find(var!User.name.notEqual("Peter")).map!(u => u.id).setEqual([1, 2, 3, 4, 5, 6]));
	assert(db.find(var!User.age.greaterEqual(33)).map!(u => u.id).setEqual([1, 2, 5, 6, 7]));
	assert(db.find(var!User.age.greater(33)).map!(u => u.id).setEqual([2, 5, 6, 7]));
	assert(db.find(var!User.age.less(33)).map!(u => u.id).setEqual([0, 3, 4]));
	assert(db.find(var!User.age.lessEqual(33)).map!(u => u.id).setEqual([0, 1, 3, 4]));

	//
	// basic contains query
	//
	assert(db.find(var!User.friends.contains(4)).map!(u => u.name).setEqual(["Tom", "Russel"]));

	//
	// basic updates
	//
	db.update(var!User.id(2), set!User.name("Thomas"));
	assert(db.find(var!User.name("Tom")).empty);
	assert(db.findOne(var!User.name("Thomas")).id == 2);

	//db.update(var!Group.id(2), )

	//
	// owned contains query
	//	
	//assert(db.groups.find(var!Group.members.contains(var!GroupMember) & var!GroupMember.user(6)).map!(g => g.name).equal(["stitching", "cooking"]));

	import std.stdio; writefln("Driver %s tested successfully.", typeof(drv).stringof);
}

@tableDefinition
struct User {
	@primaryKey int id;
	@indexed string name;
	@indexed int age;
	@unordered User[] friends;
}

@tableDefinition
struct Group {
	@primaryKey string name;
	@owned @unordered GroupMember[] members;
}

@tableDefinition @ownedBy!Group
struct GroupMember {
	User user;
	string role;
}

struct Tables {
	User users;
	Group groups;
	GroupMember groupMembers;
}

private bool setEqual(R, EL)(R range, EL[] elements)
{
	foreach (itm; range)
		if (!elements.canFind(itm))
			return false;
	return true;
}


/*User {
	int id;
	string name;
	int age;
}

User_friends {
	int user_id;
	int friend_id;
}

Group {
	string name;
}

Group_members {
	string group_name;
	int groupMember_user;
	string groupMember_role;
}*/