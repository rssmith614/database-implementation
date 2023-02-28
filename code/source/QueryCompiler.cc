#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {
	// build the tree bottom-up

	// create a SCAN operator for each table in the query
	int nTbl = 0;
	for (TableList* node = _tables; node != NULL; node = node->next) nTbl += 1;

	// leaves
	RelationalOp** forest = new RelationalOp*[nTbl];
	Schema* forestSchema = new Schema[nTbl];
	int idx = 0;
	for (TableList* node = _tables; node != NULL; node = node->next) {
		SString s(node->tableName);
		bool b = catalog->GetSchema(s, forestSchema[idx]);
		if (false == b) {
			cout << "Semantic error: table " << s << " does not exist in the database!" << endl;
			exit(1);
		}

		DBFile dbFile;
		forest[idx] = new Scan(forestSchema[idx], dbFile, s);
		idx += 1;
	}

	cout << "SCANS" << endl;
	cout << "+++++++++++++++++++++++" << endl;
	for (int i = 0; i < nTbl; i++) cout << *forest[i] << endl;


	// push-down selections: create a SELECT operator wherever necessary
	// needed when predicate compares table attribute with constant or another attribute from same table
	for (int i = 0; i < nTbl; i++) {
		Record literal;
		CNF cnf;
		int ret = cnf.ExtractCNF (*_predicate, forestSchema[i], literal);
		if (0 > ret) exit(1);

		// ret is positive, there is a condition on the schema
		// the current table needs a SELECT operator
		if (0 < ret) {
			// 							  schema	  	 condition	constants  scan (leaf node)
			RelationalOp* op = new Select(forestSchema[i],	cnf,	literal,	forest[i]);
			// replace leaf (scan) with new select operator
			forest[i] = op;
			// before: [SCAN]
			// after: [SELECT] -> [SCAN]
		}
	}

	cout << endl << "PUSH DOWN SELECTION" << endl;
	cout << "+++++++++++++++++++++++" << endl;
	for (int i = 0; i < nTbl; i++) cout << *forest[i] << endl;


	// call the optimizer to compute the join order
	OptimizationTree* root = new OptimizationTree;
	optimizer->Optimize(_tables, _predicate, root);

	// create join operators based on the optimal order computed by the optimizer
	// need nTbl - 1 Join operators
	// new Join(schemaLeft, schemaRight, schemaOut, predicate, left, right)

	// create the remaining operators based on the query
	if (_groupingAtts != NULL) {
		// create GroupBy operators (always only a single aggregate)
		// schemaIn = schema of last table in the forest
		// sumSchema = new Schema with single attribute of SUM function
		// for att in _groupingAtts
			// numAttsOutput += 1
			// idx = schemaIn.Index(att)
			// keepMe.Append(idx)
		// schemaOut = schemaIn
		// schemaOut = schemaOut.Project(keepMe)
		// sumSchema.Append(schema of _groupingAtts based on keepMe)
		// compute.GrowFromParseTree(_finalFunction, schemaIn)
		// groupingAtts = new OrderMaker(schemaIn, keepMe, keepMe.Length())
		// new GroupBy(schemaIn, schemaOut, groupingAtts, compute, producer)
	} else if (_finalFunction != NULL /* but _groupingAtts IS null */) {
		// create Sum operator
		// schemaIn = schema of last table in the forest
		// schemaOut = new Schema with single attribute
		// compute.GrowFromParseTree(_finalFunction, schemaIn)
		// new Sum(schemaIn, schemaOut, compute, producer)
	} else {
		// create Project operators
		// schemaIn = schema of last table in the forest
		// for att in _attsToSelect
			// numAttsOutput += 1
			// idx = schemaIn.Index(att)
			// keepMe.Append(idx)
		// schemaOut = schemaIn
		// schemaOut = schemaOut.Project(keepMe)
		// new Project(schemaIn, schemaOut, numAttsInput, numAttsOutput, keepMe, producer)
		if (_distinctAtts != 0) {
			// schema = schema of the Project you just made
			// producer = the new Project you just made
			// new DuplicateRemoval(schema, producer)
		}
	}

	// connect everything in the query execution tree and return
	// The root will be a WriteOut operator
	// _outfile is path to text file where query results are printed

	// free the memory occupied by the parse tree since it is not necessary anymore

	delete [] forestSchema;
}
