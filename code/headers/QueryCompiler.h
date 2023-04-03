#ifndef _QUERY_COMPILER_H
#define _QUERY_COMPILER_H

/* Take as input the query tokens produced by the query parser and generate
 * the query execution tree. This requires instantiating relational operators
 * with the correct parameters, based on the query.
 * Two issues have to be addressed:
 *  1) Identify the schema(s) for each operator.
 *  2) Identify the parameters of the operation the operator is executing.
 *     For example, identify the predicate in a SELECT. Or the JOIN PREDICATE.
 */
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

using namespace std;


class QueryCompiler {
private:
	Catalog* catalog;

	void GreedyJoin(Schema* forestSchema, int& nTbl, AndList* _predicate, RelationalOp** forest);
	void CreateScans(Schema* forestSchema, RelationalOp** forest, int nTbl, TableList* tables);
	void CreateSelects(Schema* forestSchema, RelationalOp** forest, int nTbl, AndList* predicate);
	void CreateGroupBy(Schema& saplingSchema, RelationalOp* &sapling, NameList* groupingAtts, FuncOperator* finalFunction);
	void CreateFunction(Schema& saplingSchema, RelationalOp* &sapling, FuncOperator* finalFunction);
	void CreateProject(Schema& saplingSchema, RelationalOp* &sapling, NameList* attsToSelect, int& distinctAtts);
	void PushProject(Schema* forestSchema, RelationalOp** forest, int nTbl, NameList* attsToSelect, 
					AndList* predicate, NameList* groupingAtts, FuncOperator* finalFunction);

public:
	QueryCompiler(Catalog& _catalog);
	virtual ~QueryCompiler();

	void Compile(TableList* _tables, NameList* _attsToSelect,
		FuncOperator* _finalFunction, AndList* _predicate,
		NameList* _groupingAtts, int& _distinctAtts,
		QueryExecutionTree& _queryTree);
};

#endif // _QUERY_COMPILER_H
