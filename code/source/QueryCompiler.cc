#include "QueryCompiler.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

#include <vector>
#include <set>

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog) : catalog(&_catalog) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::GreedyJoin(Schema* forestSchema, int& nTbl, AndList* _predicate, RelationalOp** forest){
	while (nTbl > 1) {
		pair<pair<int, int>, int> minJoinCost = make_pair(make_pair(0,0), numeric_limits<int>::max());
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
					int cost = noTuples * (forestSchema[idx_R].GetNumAtts() + forestSchema[idx_S].GetNumAtts());
					if (cost < minJoinCost.second) {
						minJoinCost = make_pair(make_pair(idx_R, idx_S), cost);
					}
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
			schemaOut.Append(forestSchema[idx_R]);
			schemaOut.Append(forestSchema[idx_S]);
			SInt noTuples = minJoinCost.second / schemaOut.GetNumAtts();
			schemaOut.SetNoTuples(noTuples);
			// new join op replaces lower idx
			// iterate through cnf.andlist
			bool hasJoined = false;
			for (int and_idx=0; and_idx < cnf.numAnds; and_idx++) {
				// if we see anything other than EQUALS, use NLJ
				if (cnf.andList[and_idx].op != Equals) {
					forest[idx_R] = new NestedLoopJoin(forestSchema[idx_R], forestSchema[idx_S], schemaOut, cnf, forest[idx_R], forest[idx_S]);
					hasJoined = true;
					cout << "NLJ" << endl;
				}
			}
			// check forestSchema[idx_R and S].GetNoTuples to see if >1000
			if(!hasJoined){
				if ((forestSchema[idx_R].GetNoTuples() + forestSchema[idx_S].GetNoTuples()) > 1000) {
					// use SHJ
					forest[idx_R] = new SymmetricHashJoin(forestSchema[idx_R], forestSchema[idx_S], schemaOut, cnf, forest[idx_R], forest[idx_S]);
					cout << "SHJ" << endl;
				}
				else {
					// use HJ
					forest[idx_R] = new HashJoin(forestSchema[idx_R], forestSchema[idx_S], schemaOut, cnf, forest[idx_R], forest[idx_S]);
					cout << "HJ" << endl;
				}
			}
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

void QueryCompiler::CreateScans(Schema* forestSchema, RelationalOp** forest, int nTbl, TableList* tables) {
	int idx = 0;
	for (TableList* node = tables; node != NULL; node = node->next) {
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
			SString fPath;
			ret = catalog->GetDataFile(s, fPath);
			if (ret) {
				forestSchema[idx].SetDataFile(fPath);
			}
		}

		SString dbFileLoc;
		catalog->GetDataFile(s, dbFileLoc);
		DBFile dbFile;
		dbFile.Open((char*)((string) dbFileLoc).c_str());
		forest[idx] = new Scan(forestSchema[idx], dbFile, s);
		idx += 1;
	}
}

void QueryCompiler::CreateIndexScans(Schema* forestSchema, RelationalOp** forest, int nTbl, TableList* _tables, AndList* _predicate) {

	SString dbFileName;
	DBFile dbFile;

	SString indexFileName;
	BTreeIndex indexFile;

	// all tables in query
	for (int i=0; i < nTbl; i++) {
		CNF cnf;
		Record constants;
		int ret = cnf.ExtractCNF(*_predicate, forestSchema[i], constants);

		SInt lower(-1), upper(-1);
		int idxOfIdxAtt = -1;

		if (ret > 0) {
			// every predicate from WHERE
			for (int j=0; j < cnf.numAnds; j++) {
				auto condition = cnf.andList[j];
				int idxOfAttInSchema = -1;
				int idxOfConstInRec = -1;

				// find attribute from predicate
				if (condition.operand1 == Left && condition.operand2 == Literal) {
					idxOfAttInSchema = condition.whichAtt1;
					idxOfConstInRec = condition.whichAtt2;
				} else if (condition.operand1 == Literal && condition.operand2 == Right) {
					idxOfAttInSchema = condition.whichAtt2;
					idxOfConstInRec = condition.whichAtt1;
				} else {
					continue;
				}

				SString attName;
				forestSchema[i].Name(attName, idxOfAttInSchema);

				// int test = forestSchema[i].Index(attName);

				string tableName, indexName;

				// see if we have an index we can use on the attribute
				if (catalog->HasIndex(attName, tableName, indexName)) {
					SString tName(tableName);
					SString dataFile;
					catalog->GetDataFile(tName, dataFile);

					dbFile.Open((char*)((string) dataFile).c_str());

					indexFileName = "../data/" + indexName + ".idx";
					indexFile.Read((string) indexFileName);
				} else {
					continue;
				}

				idxOfIdxAtt = idxOfAttInSchema;

				if (condition.op == LessThan) {
					upper = ((int*) constants.GetColumn(idxOfConstInRec))[0];
				} else if (condition.op == GreaterThan) {
					lower = ((int*) constants.GetColumn(idxOfConstInRec))[0];
				} else if (condition.op == Equals) {
					upper = -2;
					lower = ((int*) constants.GetColumn(idxOfConstInRec))[0];
				}

			}
			if (idxOfIdxAtt != -1) {
				forest[i] = new IndexScan(dbFile, indexFile, lower, upper, idxOfIdxAtt);
				idxOfIdxAtt = -1;
			}
		}
	}
}

void QueryCompiler::CreateSelects(Schema* forestSchema, RelationalOp** forest, int nTbl, AndList* predicate) {
	for (int i = 0; i < nTbl; i++) {
		Record literal;
		CNF cnf;
		int ret = cnf.ExtractCNF (*predicate, forestSchema[i], literal);
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
			// if (dynamic_cast<IndexScan*>(forest[i])) {
			// 	break;
			// }
			// 							  schema	  	 condition	constants  scan (leaf node)
			RelationalOp* op = new Select(forestSchema[i],	cnf,	literal,	forest[i]);
			// replace leaf (scan) with new select operator
			forest[i] = op;
			// before: [SCAN]
			// after: [SELECT] -> [SCAN]
		}
	}
}

void QueryCompiler::CreateGroupBy(Schema& saplingSchema, RelationalOp* &sapling, NameList* groupingAtts, FuncOperator* finalFunction) {
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
	for (NameList* att = groupingAtts; att != NULL; att = att->next){
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
	compute.GrowFromParseTree(finalFunction, schemaIn);

	// OrderMaker constructor takes int*, not IntVector
	int* keepMe_intv = new int[numAttsOutput];
	for (int i=0; i<numAttsOutput; i++) {
		keepMe_intv[i] = keepMe[i];
	}

	OrderMaker groupingAttsOrderMaker(schemaIn, keepMe_intv, numAttsOutput);

	delete [] keepMe_intv;

	RelationalOp* producer = sapling;
	sapling = new GroupBy(schemaIn, schemaOut, groupingAttsOrderMaker, compute, producer);

	saplingSchema.Swap(sumSchema);
}

void QueryCompiler::CreateFunction(Schema& saplingSchema, RelationalOp* &sapling, FuncOperator* finalFunction) {
	// create Sum operator
	// schemaIn = schema of last table in the forest
	Schema schemaIn = saplingSchema;
	
	// schemaOut = new Schema with single attribute
	StringVector atts; SString attName("sum"); atts.Append(attName);
	StringVector attTypes; SString attType("FLOAT"); attTypes.Append(attType);
	IntVector distincts; SInt dist(1); distincts.Append(dist);
	Schema schemaOut(atts, attTypes, distincts);

	// Sum function
	Function compute;
	compute.GrowFromParseTree(finalFunction, schemaIn);

	RelationalOp* producer = sapling;
	sapling = new Sum(schemaIn, schemaOut, compute, producer);

	saplingSchema.Swap(schemaOut);
}

void QueryCompiler::CreateProject(Schema& saplingSchema, RelationalOp* &sapling, NameList* attsToSelect, int& distinctAtts) {
	// create Project operators
	// schemaIn = schema of last table in the forest
	Schema schemaIn = saplingSchema;
	int numAttsInput = schemaIn.GetNumAtts();
	
	// schemaOut based on indexes of atts
	Schema schemaOut = schemaIn;
	IntVector keepMe;
	for (NameList* att = attsToSelect; att != NULL; att = att->next) {
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

	// delete [] keepMe_intv;

	if (distinctAtts != 0) {
		// schema = schema of the Project you just made

		int noTuples = schemaOut.GetNoTuples();
		int ndv_prod = 1;
		for (int i=0; i<schemaOut.GetAtts().Length(); i++) {
			SString attName(schemaOut.GetAtts()[i].name);
			ndv_prod *= schemaOut.GetDistincts(attName);
		}
		SInt cardinality(min(ceil(noTuples/2.), (double) ndv_prod));
		schemaOut.SetNoTuples(cardinality);
		// producer = the new Project you just made
		RelationalOp* producer = sapling;
		sapling = new DuplicateRemoval(schemaOut, producer);
	}

	saplingSchema.Swap(schemaOut);
}

void QueryCompiler::PushProject(Schema* forestSchema, RelationalOp** forest, int nTbl, NameList* attsToSelect, 
	AndList* predicate, NameList* groupingAtts, FuncOperator* finalFunction) {

	set<string> neededAtts;

	// all attributes needed from SELECT clause
	for (NameList* name = attsToSelect; name != NULL; name = name->next) {
		neededAtts.insert(name->name);
	}

	// all attributes from GROUP BY clause
	if (groupingAtts != NULL) {
		for (NameList* name = groupingAtts; name != NULL; name = name->next) {
			neededAtts.insert(name->name);
		}
	}

	// all attributes from inside the function
	// DFS on parse tree
	if (finalFunction != NULL) {
		vector<FuncOperator*> function_stack;
		function_stack.push_back(finalFunction);
		while (!function_stack.empty()) {
			FuncOperator* cur = function_stack.back(); function_stack.pop_back();
			// only want attribute names, no constants
			if (cur->leftOperand != NULL) {
				if (cur->leftOperand->code == NAME)
					neededAtts.insert(cur->leftOperand->value);
			}
			if (cur->leftOperator != NULL) {
				function_stack.push_back(cur->leftOperator);
			}
			if (cur->right != NULL) {
				function_stack.push_back(cur->right);
			}
		}
	}

	// all attributes needed from WHERE clause
	// DFS on parse tree
	vector<AndList*> where_stack;
	where_stack.push_back(predicate);
	while (!where_stack.empty()) {
		AndList* cur = where_stack.back(); where_stack.pop_back();
		// only want attribute names, no constants
		if (cur->left->left->code == NAME) {
			neededAtts.insert(cur->left->left->value);
		}
		if (cur->left->right->code == NAME) {
			neededAtts.insert(cur->left->right->value);
		}
		if (cur->rightAnd != NULL) {
			where_stack.push_back(cur->rightAnd);
		}
	}

	// create a proejction for every table
	for (int i=0; i < nTbl; i++) {
		Schema schemaIn = forestSchema[i];
		int numAttsInput = schemaIn.GetNumAtts();
		IntVector keepMe;
		for (int j=0; j < numAttsInput; j++) {
			string currentAtt = schemaIn.GetAtts()[j].name;
			// keep only attributes needed by the query
			if (neededAtts.find(currentAtt) != neededAtts.end()) {
				SInt idx(j);
				keepMe.Append(idx);
			}
		}
		Schema schemaOut = schemaIn;
		schemaOut.Project(keepMe);

		int numAttsOutput = keepMe.Length();

		// only create the projection if the projection would actually
		// reduce the number of columns, otherwise we'd be bloating
		// the query execution with more function calls
		if (numAttsInput > numAttsOutput) {
			// Project constructor takes int*, not IntVector
			int* keepMe_intv = new int[numAttsOutput];
			for (int i=0; i<numAttsOutput; i++) {
				keepMe_intv[i] = keepMe[i];
			}
			RelationalOp* producer = forest[i];
			forest[i] = new Project(schemaIn, schemaOut, numAttsInput, numAttsOutput, keepMe_intv, producer);

			forestSchema[i].Swap(schemaOut);
		}
	}
}

// in-place linked list reversal
void reverseList(NameList*& head) {
    NameList* prev = NULL;
    NameList* curr = head;
	NameList* next = NULL;
    while (curr != NULL) {
        next = curr->next;
        curr->next = prev;
        prev = curr;
        curr = next;
    }
    head = prev;
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {
	// build the tree bottom-up

	// parser reads SELECT atts backwards
	// reverse them so the output comes in the desired order
	reverseList(_attsToSelect);

	// create a SCAN operator for each table in the query
	int nTbl = 0;
	for (TableList* node = _tables; node != NULL; node = node->next) nTbl += 1;

	// leaves
	RelationalOp** forest = new RelationalOp*[nTbl];
	Schema* forestSchema = new Schema[nTbl];
	
	CreateScans(forestSchema, forest, nTbl, _tables);

	CreateIndexScans(forestSchema, forest, nTbl, _tables, _predicate);

	// push-down selections: create a SELECT operator wherever necessary
	// needed when predicate compares table attribute with constant or another attribute from same table
	CreateSelects(forestSchema, forest, nTbl, _predicate);

	// push down projection: create PROJECT operators to help reduce
	// intermediate table sizes for large queries
	if (nTbl > 1)
		PushProject(forestSchema, forest, nTbl, _attsToSelect, _predicate, _groupingAtts, _finalFunction);

	// create join operators based on the optimal order computed by the optimizer
	// need nTbl - 1 Join operators
	GreedyJoin(forestSchema, nTbl, _predicate, forest);

	// after joins, there is only one node in the forest
	RelationalOp* sapling = forest[0];
	Schema saplingSchema = forestSchema[0];
	
	// create the remaining operators based on the query
	if (_groupingAtts != NULL) {
		CreateGroupBy(saplingSchema, sapling, _groupingAtts, _finalFunction);
	} else if (_finalFunction != NULL /* but _groupingAtts IS null */) {
		CreateFunction(saplingSchema, sapling, _finalFunction);
	} else if (_attsToSelect != NULL) {
		CreateProject(saplingSchema, sapling, _attsToSelect, _distinctAtts);
	}

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
