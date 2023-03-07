#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include <map>

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void GreedyJoin(Schema* forestSchema, int nTbl, AndList* _predicate, RelationalOp** forest){
	while (nTbl > 1) {
		pair<pair<int, int>, int> minJoinCost = make_pair(make_pair(0,0), INT_MAX);
		for (int idx_R=0; idx_R < nTbl-1; idx_R++) {
			for (int idx_S=idx_R+1; idx_S < nTbl; idx_S++) {
				CNF cnf;
				int ret = cnf.ExtractCNF(*_predicate, forestSchema[idx_R], forestSchema[idx_S]);
				if (ret > 0) {
					// (|R| * |S|) / max(NDV(R, joinAtt), NDV(S, joinAtt))
					int card_R = forestSchema[idx_R].GetNoTuples();
					int card_S = forestSchema[idx_S].GetNoTuples();

					int idxOfJoinAtt_R = -1, idxOfJoinAtt_S = -1;
					
					int divisor = 1;
					for (int and_idx=0; and_idx < cnf.numAnds; and_idx++) {
						// operand1 = Left means left side of join condition belongs to R
						if (cnf.andList[and_idx].operand1 == Left) {
							idxOfJoinAtt_R = cnf.andList[and_idx].whichAtt1;
							idxOfJoinAtt_S = cnf.andList[and_idx].whichAtt2;
						// Right means it belongs to S
						} else { // operand2 = Right
							idxOfJoinAtt_R = cnf.andList[and_idx].whichAtt2;
							idxOfJoinAtt_S = cnf.andList[and_idx].whichAtt1;
						}
						SString joinAttName_R = forestSchema[idx_R].GetAtts()[idxOfJoinAtt_R].name;
						SString joinAttName_S = forestSchema[idx_S].GetAtts()[idxOfJoinAtt_S].name;
						int ndv_R = forestSchema[idx_R].GetDistincts(joinAttName_R);
						int ndv_S = forestSchema[idx_S].GetDistincts(joinAttName_S);

						divisor *= max(ndv_R, ndv_S);
					}

					int noTuples = (card_R * card_S) / divisor;
					if (noTuples < minJoinCost.second) {
						minJoinCost = make_pair(make_pair(idx_R, idx_S), noTuples);
					}
					// joinCardialities.insert(make_pair(make_pair(idx_R, idx_S), noTuples));
				}
			}
		}
		// use minJoinCost.first to join two operators
		int idx_R = minJoinCost.first.first;
		int idx_S = minJoinCost.first.second;
		CNF cnf;
		int ret = cnf.ExtractCNF(*_predicate, forestSchema[idx_R], forestSchema[idx_S]);
		if (ret > 0){
			Schema schemaOut;
			schemaOut.Swap(forestSchema[idx_R]);
			schemaOut.Append(forestSchema[idx_S]);
			SInt noTuples = minJoinCost.second;
			schemaOut.SetNoTuples(noTuples);
			// new join op replaces lower idx
			forest[idx_R] = new NestedLoopJoin(forestSchema[idx_R], forestSchema[idx_S], schemaOut, cnf, forest[idx_R], forest[idx_S]);
			forestSchema[idx_R].Swap(schemaOut);
			// ops above higher idx are shifted down
			for (int j=idx_S; j < nTbl-1; j++) {
				forest[j] = forest[j+1];
				forestSchema[j].Swap(forestSchema[j+1]);
			}
			nTbl--;
			continue;
		}
	}
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

		SInt noTuples;
		catalog->GetNoTuples(s, noTuples);
		forestSchema[idx].SetNoTuples(noTuples);

		for (int i=0; i<forestSchema[idx].GetAtts().Length(); i++) {
			SString att = forestSchema[idx].GetAtts()[i].name;
			SInt noDist;
			bool ret = catalog->GetNoDistinct(s, att, noDist);
			if (ret) {
				forestSchema[idx].SetDistincts(att, noDist);
			}
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
			SInt noTuples = forestSchema[i].GetNoTuples();
			// noTuples /= NDV for point query
			// noTuples /= 3 for range query
			for (int j=0; j < cnf.numAnds; j++) {
				if (cnf.andList[j].op == Equals) {
					int idxOfAttInSchema = -1;
					if (cnf.andList[j].operand1 == Left) {
						idxOfAttInSchema = cnf.andList[j].whichAtt1;
					} else {
						idxOfAttInSchema = cnf.andList[j].whichAtt2;
					}
					SString attName = forestSchema[i].GetAtts()[idxOfAttInSchema].name;
					noTuples = noTuples / forestSchema[i].GetDistincts(attName);
				} else {
					noTuples = noTuples / 3;
				}
			}
			forestSchema[i].SetNoTuples(noTuples);
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

	// create join operators based on the optimal order computed by the optimizer
	// need nTbl - 1 Join operators
	GreedyJoin(forestSchema, nTbl, _predicate, forest);
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++
	//Delete below if method works
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++
	// while (nTbl > 1) {
	// 	pair<pair<int, int>, int> minJoinCost = make_pair(make_pair(0,0), INT_MAX);
	// 	for (int idx_R=0; idx_R < nTbl-1; idx_R++) {
	// 		for (int idx_S=idx_R+1; idx_S < nTbl; idx_S++) {
	// 			CNF cnf;
	// 			int ret = cnf.ExtractCNF(*_predicate, forestSchema[idx_R], forestSchema[idx_S]);
	// 			if (ret > 0) {
	// 				// (|R| * |S|) / max(NDV(R, joinAtt), NDV(S, joinAtt))
	// 				int card_R = forestSchema[idx_R].GetNoTuples();
	// 				int card_S = forestSchema[idx_S].GetNoTuples();

	// 				int idxOfJoinAtt_R = -1, idxOfJoinAtt_S = -1;
					
	// 				int divisor = 1;
	// 				for (int and_idx=0; and_idx < cnf.numAnds; and_idx++) {
	// 					// operand1 = Left means left side of join condition belongs to R
	// 					if (cnf.andList[and_idx].operand1 == Left) {
	// 						idxOfJoinAtt_R = cnf.andList[and_idx].whichAtt1;
	// 						idxOfJoinAtt_S = cnf.andList[and_idx].whichAtt2;
	// 					// Right means it belongs to S
	// 					} else { // operand2 = Right
	// 						idxOfJoinAtt_R = cnf.andList[and_idx].whichAtt2;
	// 						idxOfJoinAtt_S = cnf.andList[and_idx].whichAtt1;
	// 					}
	// 					SString joinAttName_R = forestSchema[idx_R].GetAtts()[idxOfJoinAtt_R].name;
	// 					SString joinAttName_S = forestSchema[idx_S].GetAtts()[idxOfJoinAtt_S].name;
	// 					int ndv_R = forestSchema[idx_R].GetDistincts(joinAttName_R);
	// 					int ndv_S = forestSchema[idx_S].GetDistincts(joinAttName_S);

	// 					divisor *= max(ndv_R, ndv_S);
	// 				}

	// 				int noTuples = (card_R * card_S) / divisor;
	// 				if (noTuples < minJoinCost.second) {
	// 					minJoinCost = make_pair(make_pair(idx_R, idx_S), noTuples);
	// 				}
	// 				// joinCardialities.insert(make_pair(make_pair(idx_R, idx_S), noTuples));
	// 			}
	// 		}
	// 	}
	// 	// use minJoinCost.first to join two operators
	// 	int idx_R = minJoinCost.first.first;
	// 	int idx_S = minJoinCost.first.second;
	// 	CNF cnf;
	// 	int ret = cnf.ExtractCNF(*_predicate, forestSchema[idx_R], forestSchema[idx_S]);
	// 	if (ret > 0){
	// 		Schema schemaOut;
	// 		schemaOut.Swap(forestSchema[idx_R]);
	// 		schemaOut.Append(forestSchema[idx_S]);
	// 		SInt noTuples = minJoinCost.second;
	// 		schemaOut.SetNoTuples(noTuples);
	// 		// new join op replaces lower idx
	// 		forest[idx_R] = new NestedLoopJoin(forestSchema[idx_R], forestSchema[idx_S], schemaOut, cnf, forest[idx_R], forest[idx_S]);
	// 		forestSchema[idx_R].Swap(schemaOut);
	// 		// ops above higher idx are shifted down
	// 		for (int j=idx_S; j < nTbl-1; j++) {
	// 			forest[j] = forest[j+1];
	// 			forestSchema[j].Swap(forestSchema[j+1]);
	// 		}
	// 		nTbl--;
	// 		continue;
	// 	}
	// }

	cout << endl << "JOINS" << endl;
	cout << "+++++++++++++++++++++++" << endl;
	for (int i = 0; i < nTbl; i++) cout << *forest[i] << endl;

	// after joins, 
	RelationalOp* sapling = forest[0];
	Schema saplingSchema = forestSchema[0];
	
	// create the remaining operators based on the query
	if (_groupingAtts != NULL) {
		// create GroupBy operators (always only a single aggregate)
		Schema schemaIn = saplingSchema;
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
		sumSchema.SetNoTuples(schemaOut.GetNoTuples());

		int noTuples = sumSchema.GetNoTuples();
		int ndv_prod = 1;
		for (int i=0; i<sumSchema.GetAtts().Length(); i++) {
			SString attName(sumSchema.GetAtts()[i].name);
			ndv_prod *= sumSchema.GetDistincts(attName);
		}
		SInt cardinality(min(noTuples/2, ndv_prod));
		sumSchema.SetNoTuples(cardinality);

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

		saplingSchema.Swap(sumSchema);

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

		saplingSchema.Swap(schemaOut);

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

		delete [] keepMe_intv;

		// distinct
		if (_distinctAtts != 0) {
			// schema = schema of the Project you just made
			// Schema schema = saplingSchema;

			int noTuples = schemaOut.GetNoTuples();
			int ndv_prod = 1;
			for (int i=0; i<schemaOut.GetAtts().Length(); i++) {
				SString attName(schemaOut.GetAtts()[i].name);
				ndv_prod *= schemaOut.GetDistincts(attName);
			}
			SInt cardinality(min(noTuples/2, ndv_prod));
			schemaOut.SetNoTuples(cardinality);
			// producer = the new Project you just made
			RelationalOp* producer = sapling;
			sapling = new DuplicateRemoval(schemaOut, producer);
		}

		saplingSchema.Swap(schemaOut);
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
}
