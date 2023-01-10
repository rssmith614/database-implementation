#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "Catalog.h"
extern "C" {
#include "QueryParser.h"
}
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "Record.h"
#include "RelOp.h"
#include "Comparison.h"
#include "Map.h"
#include "Map.cc"

using namespace std;


// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern "C" int yyparse();
extern "C" int yylex_destroy();


int main () {
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler
	QueryOptimizer optimizer(catalog);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);


	// the query parser is accessed directly through yyparse
	// this populates the extern data structures
	int parse = -1;
	if (yyparse () == 0) {
		cout << "OK!" << endl;
		parse = 0;
	}
	else {
		cout << "Error: Query is not correct!" << endl;
		parse = -1;
	}

	yylex_destroy();

	if (parse != 0) return -1;

	// at this point we have the parse tree in the ParseTree data structures
	// we are ready to invoke the query compiler with the given query
	// the result is the execution tree built from the parse tree and optimized
	QueryExecutionTree queryTree;
	compiler.Compile(tables, attsToSelect, finalFunction, predicate,
		groupingAtts, distinctAtts, queryTree);

	cout << queryTree << endl;

	Map<KeyInt, KeyInt> map;
	KeyInt k1(1), k2(1), k3(2), k4(1), k5(1), k6(2);
	KeyInt v1(0), v2(0), v3(1), v4(1), v5(3), v6(5);
	map.Insert(k1, v1);
	map.Insert(k2, v2);
	map.Insert(k3, v3);
	map.Insert(k4, v4);
	map.Insert(k5, v5);
	map.Insert(k6, v6);

	for (map.MoveToStart(); map.AtEnd() == false; map.Advance()) {
		int k = map.CurrentKey();
		int v = map.CurrentData();

		printf("%d %d\n", k, v);
	}

	printf("+++++++++++++++++++++++++++++++++++++++++++++++\n");
	
	KeyInt k(1);
	map.IsThere(k);
	while (map.CurrentKey() == k) {
		int kk = map.CurrentKey();
		int vv = map.CurrentData();

		printf("%d %d\n", kk, vv);
		map.Advance();		
	}

	return 0;
}
