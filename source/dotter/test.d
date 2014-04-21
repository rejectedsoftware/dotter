/**
	Conformance tests for driver implementations.

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.test;

import dotter.orm;

import std.algorithm : equal, map;


void testDriver(alias CREATE_DRIVER)()
{
	auto drv = CREATE_DRIVER!Tables();
	auto db = createORM(drv);

	db.users.insert(0, "Dummy", 0);
	db.users.removeAll();
	assert(db.users.find().map!(u => u.toTuple()).empty);

	db.users.insert(0, "Peter", 21);
	db.users.insert(1, "Stacy", 33);
	db.users.insert(2, "Tom", 45);
	db.users.insert(3, "Linda", 28);
	db.users.insert(4, "Jack", 19);
	db.users.insert(5, "Russel", 52);
	db.users.insert(6, "Hans", 64);
	db.users.insert(7, "Peter", 42);

	assert(db.users.find().map!(u => u.toTuple()).equal([
		tuple(0, "Peter", 21),
		tuple(1, "Stacy", 33),
		tuple(2, "Tom", 45),
		tuple(3, "Linda", 28),
		tuple(4, "Jack", 19),
		tuple(5, "Russel", 52),
		tuple(6, "Hans", 64),
		tuple(7, "Peter", 42)
	]));

	db.groupMembers.removeAll();
	assert(db.groupMembers.find().map!(gm => gm.toTuple()).empty);

	db.groupMembers.insert(0, 1, "admin");
	db.groupMembers.insert(1, 1, "member");
	db.groupMembers.insert(2, 0, "member");
	db.groupMembers.insert(3, 2, "member");
	db.groupMembers.insert(4, 3, "admin");
	db.groupMembers.insert(5, 4, "member");
	db.groupMembers.insert(6, 3, "admin");
	db.groupMembers.insert(7, 4, "member");
	db.groupMembers.insert(8, 5, "member");
	db.groupMembers.insert(9, 6, "member");
	db.groupMembers.insert(10, 7, "member");
	db.groupMembers.insert(11, 7, "member");

	assert(db.groupMembers.find().map!(gm => gm.toTuple()).equal([
		tuple(0, 1, "admin"),
		tuple(1, 1, "member"),
		tuple(2, 0, "member"),
		tuple(3, 2, "member"),
		tuple(4, 3, "admin"),
		tuple(5, 4, "member"),
		tuple(6, 3, "admin"),
		tuple(7, 4, "member"),
		tuple(8, 5, "member"),
		tuple(9, 6, "member"),
		tuple(10, 7, "member"),
		tuple(11, 7, "member")
	]));

	db.groups.removeAll();
	assert(db.groups.find().map!(g => g.toTuple()).empty);

	db.groups.insert("computer", [0, 2, 3, 8]);
	db.groups.insert("stitching", [1, 4, 5, 9]);
	db.groups.insert("cooking", [6, 7, 10, 11]);

	assert(db.groups.find().map!(g => g.toTuple()).equal([
		tuple("computer", [0, 2, 3, 8]),
		tuple("stitching", [1, 4, 5, 9]),
		tuple("cooking", [6, 7, 10, 11])
	]));

	//
	// basic queries
	//
	assert(db.find(var!User.name("Peter")).map!(u => u.id).equal([0, 7]));
	assert(db.find(var!User.name.equal("Peter")).map!(u => u.id).equal([0, 7]));
	assert(db.find(var!User.name.notEqual("Peter")).map!(u => u.id).equal([1, 2, 3, 4, 5, 6]));
	assert(db.find(var!User.age.greaterEqual(33)).map!(u => u.id).equal([1, 2, 5, 6, 7]));
	assert(db.find(var!User.age.greater(33)).map!(u => u.id).equal([2, 5, 6, 7]));
	assert(db.find(var!User.age.less(33)).map!(u => u.id).equal([0, 3, 4]));
	assert(db.find(var!User.age.lessEqual(33)).map!(u => u.id).equal([0, 1, 3, 4]));

	//
	// basic contains query
	//
	assert(db.find(var!Group.members.contains(1)).map!(g => g.name).equal(["stitching"]));
}

@tableDefinition
struct User {
	@primaryKey int id;
	string name;
	int age;
}

@tableDefinition
struct Group {
	@primaryKey string name;
	@owned GroupMember[] members;
}

@tableDefinition
struct GroupMember {
	@primaryKey int id;
	User user;
	string role;
}

struct Tables {
	User users;
	Group groups;
	GroupMember groupMembers;
}