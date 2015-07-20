module dotter.drivers.sql;

import dotter.orm;

import std.traits;


template sqlQueryExpression(SQL, QUERY)
	if (isInstanceOf!(QueryAnyExpr, QUERY))
{
	enum sqlQueryExpression = "";
}

template sqlQueryExpression(SQL, QUERY)
	if (isInstanceOf!(ConjunctionExpr, QUERY))
{
	static string compute()
	{
		string ret;
		foreach (i, E; typeof(QUERY.exprs)) {
			if (ret.length) ret ~= " " ~ SQL.andOperator ~ " ";
			ret ~= sqlQueryExpression!(SQL, E);
		}
		return ret;
	}
	enum sqlQueryExpression = compute();
}

template sqlQueryExpression(SQL, QUERY)
	if (isInstanceOf!(DisjunctionExpr, QUERY))
{
	static string compute()
	{
		string ret;
		foreach (i, E; typeof(QUERY.exprs)) {
			pragma(msg, E);
			if (ret.length) ret ~= " " ~ SQL.orOperator ~ " ";
			ret ~= "(" ~ sqlQueryExpression!(SQL, E) ~ ")";
		}
		return ret;
	}
	enum sqlQueryExpression = compute();
}

template sqlQueryExpression(SQL, QUERY)
	if (isInstanceOf!(CompareExpr, QUERY))
{
	static if (QUERY.op == CompareOp.contains) static assert(false);
	else static if (QUERY.op == CompareOp.anyOf) static assert(false);
	else enum sqlQueryExpression = QUERY.name ~ " " ~ SQL.operators[QUERY.op] ~ " ?";
}
