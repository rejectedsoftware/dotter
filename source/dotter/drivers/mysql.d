/**
	MySQL based mapping driver.

	Status:
		This driver is not implemented, yet.

	Copyright: © 2014 rejectedsoftware e.K.
	License: Subject to the terms of the MIT license, as written in the included LICENSE.txt file.
	Authors: Sönke Ludwig
*/
module dotter.drivers.mysql;

version (Have_mysql_native):

import dotter.drivers.sql;
import dotter.orm;

import mysql.db;
import std.traits;


class MySQLORMDriver(TABLES) {
	alias Tables = TABLES;
	alias TableTypes = TypeTuple!(typeof(TABLES.tupleof));
	enum fieldTableNames = getFieldTableNames!TABLES();

	private {
		MysqlDB m_db;
	}

	this(string conn_str)
	{
        m_db = new MysqlDB(conn_str);
	}


}

private struct MySQLDefinitions {
	enum string[] keywords = [];
	enum orOperator = "OR";
	enum andOperator = "AND";
	enum string[CompareOp] operators = [
		CompareOp.equal : "=",
		CompareOp.notEqual: "<>",
		CompareOp.greater: ">",
		CompareOp.greaterEqual: ">=",
		CompareOp.less: "<",
		CompareOp.lessEqual: "<="
	];
}

unittest {
	@tableDefinition struct T {
		string a;
		int b;
	}
	auto q = var!T.a.equal("a") & var!T.b.greater(24) | var!T.a.notEqual("a");
	assert(sqlQueryExpression!(MySQLDefinitions, typeof(q)) == "(a = ? AND b > ?) OR (a <> ?)");
}