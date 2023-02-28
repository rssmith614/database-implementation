#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include <stack>

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

	// after joins, 
	RelationalOp* sapling = forest[0];
	Schema saplingSchema;
	
	// create the remaining operators based on the query
	if (_groupingAtts != NULL) {
		// create GroupBy operators (always only a single aggregate)
		Schema schemaIn = forestSchema[0];
		// schemaIn = schema of last table in the forest
		StringVector atts; SString attName("sum"); atts.Append(attName);
		StringVector attTypes; SString attType("FLOAT"); attTypes.Append(attType);
		IntVector distincts; SInt dist(1); distincts.Append(dist);
		Schema sumSchema(atts, attTypes, distincts);
		// sumSchema = new Schema with single attribute of SUM function
		IntVector keepMe;
		int numAttsOutput = 0;
		for (NameList* att = _groupingAtts; att != NULL; att = att->next){
			numAttsOutput += 1;
			SString attName(att->name);
			SInt idx = schemaIn.Index(attName);
			keepMe.Append(idx);
		}

		// SUM(att1), att2 -- will always do this one
		// att2, SUM(att1) -- will not handle this properly
		
		Schema schemaOut = schemaIn;
		schemaOut.Project(keepMe);
		sumSchema.Append(schemaOut);

		// Sum function
		Function compute;
		compute.GrowFromParseTree(_finalFunction, schemaIn);

		// OrderMaker constructor takes int*, not IntVector
		int* keepMe_intv = new int[numAttsOutput];
		for (int i=0; i<numAttsOutput; i++) {
			keepMe_intv[i] = keepMe[i];
		}
	
		OrderMaker groupingAtts(schemaIn, keepMe_intv, keepMe.Length());

		delete [] keepMe_intv;

		RelationalOp* producer = sapling;
		sapling = new GroupBy(schemaIn, sumSchema, groupingAtts, compute, producer);

		saplingSchema = sumSchema;

	} else if (_finalFunction != NULL /* but _groupingAtts IS null */) {
		// create Sum operator
		// schemaIn = schema of last table in the forest
		Schema schemaIn = forestSchema[0];
		
		// schemaOut = new Schema with single attribute
		StringVector atts; SString attName("sum"); atts.Append(attName);
		StringVector attTypes; SString attType("FLOAT"); attTypes.Append(attType);
		IntVector distincts; SInt dist(1); distincts.Append(dist);
		Schema schemaOut(atts, attTypes, distincts);

		// Sum function
		Function compute;
		compute.GrowFromParseTree(_finalFunction, schemaIn);

		RelationalOp* producer = sapling;
		sapling = new Sum(schemaIn, schemaOut, compute, producer);

		saplingSchema = schemaOut;

	} else {
		// create Project operators
		// schemaIn = schema of last table in the forest
		Schema schemaIn = forestSchema[0];
		int numAttsInput = schemaIn.GetNumAtts();
		
		// schemaOut based on indexes of atts
		Schema schemaOut = schemaIn;
		IntVector keepMe;
		for (NameList* att = _attsToSelect; att != NULL; att = att->next) {
			SString attName(att->name);
			SInt idx = schemaIn.Index(attName);
			keepMe.Append(idx);
		}
		schemaOut.Project(keepMe);
		int numAttsOutput = keepMe.Length();

		// Project constructor takes int*, not IntVector
		int* keepMe_intv = new int[numAttsOutput];
		for (int i=0; i<numAttsOutput; i++) {
			keepMe_intv[i] = keepMe[i];
		}

		RelationalOp* producer = sapling;
		sapling = new Project(schemaIn, schemaOut, numAttsInput, numAttsOutput, keepMe_intv, producer);

		saplingSchema = schemaOut;

		delete [] keepMe_intv;

		// distinct
		if (_distinctAtts != 0) {
			// schema = schema of the Project you just made
			Schema schema = saplingSchema;
			// producer = the new Project you just made
			RelationalOp* producer = sapling;
			sapling = new DuplicateRemoval(schema, producer);
		}
	}

	cout << "GROUP BY, FUNCTION, OR PROJECT" << endl;
	cout << "+++++++++++++++++++++++++++++" << endl;
	cout << *sapling << endl;

	// connect everything in the query execution tree and return
	// The root will be a WriteOut operator
	// _outfile is path to text file where query results are printed
	string outfile = "output.txt";
	sapling = new WriteOut(saplingSchema, outfile, sapling);
	_queryTree.SetRoot(*sapling);

	// free the memory occupied by the parse tree since it is not necessary anymore
	delete [] forest;
	delete [] forestSchema;
	delete root;
}
