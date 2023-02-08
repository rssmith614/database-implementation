#include "QueryCompiler.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog) : catalog(&_catalog) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	// create a SCAN operator for each table in the query

	// push-down selections: create a SELECT operator wherever necessary

	// compute the optimal join order and create corresponding join operators

	// create the remaining operators based on the query

	// connect everything in the query execution tree

	// free the memory occupied by the parse tree since it is not necessary anymore
}
