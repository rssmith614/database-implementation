#include <iostream>
#include <fstream>
#include <stack>
#include <unordered_set>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os, 0);
}


Scan::Scan(Schema& _schema, DBFile& _file, string _tblName) :
	schema(_schema), file(_file), tblName(_tblName) {
  
  file.MoveFirst();
}

Scan::~Scan() {

}

// returns true when record was retrieved
bool Scan::GetNext(Record& _record) {
	if (0 == file.GetNext(_record)) {
		return true;
	}
	return false;

	// we love one-liners
	// return 0 == file.GetNext(_record) ? true : false;
}

ostream& Scan::print(ostream& _os, int depth) {
	_os << "SCAN (" << schema.GetNoTuples() << " tuples, " << schema.GetNumAtts() << " atts, table: " << tblName << ")";
	return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer)
	{

	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;

}

Select::~Select() {
	delete producer;
}

bool Select::GetNext(Record& _record) {
	while (producer->GetNext(_record)) {
		if (predicate.Run(_record, constants)) {
			return true;
		}
		// if predicate is not satisfied, pull a new record
	}
	return false;	
}

ostream& Select::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "SELECT (" << schema.GetNoTuples() << " tuples, " << schema.GetNumAtts() << " atts, " << predicate.numAnds << " conditions)\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer):
	schemaIn(_schemaIn), schemaOut(_schemaOut), numAttsInput(_numAttsInput),
	numAttsOutput(_numAttsOutput), keepMe(_keepMe), producer(_producer) {

}

Project::~Project() {
	delete [] keepMe;
	delete producer;
}

bool Project::GetNext(Record& _record) {
	if (producer->GetNext(_record)) {
		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;
	}
	return false;
}

ostream& Project::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "PROJECT (" << schemaOut.GetNumAtts() << " atts)\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	return _os;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) :
	schemaLeft(_schemaLeft), schemaRight(_schemaRight), schemaOut(_schemaOut),
	predicate(_predicate), left(_left), right(_right) {

}

Join::~Join() {

}

ostream& Join::print(ostream& _os, int depth) {
	string tabs(depth, '\t');		
	_os << "JOIN (" << schemaOut.GetNoTuples() << " tuples, " << schemaOut.GetNumAtts() << " atts, " << predicate.numAnds << " conditions)\n" << tabs << "├────>";
	left->print(_os, depth+1);
	_os << "\n" << tabs << "└────>";
	right->print(_os, depth+1);
	return _os;
}


NestedLoopJoin::NestedLoopJoin(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right)
	: Join(_schemaLeft, _schemaRight, _schemaOut, _predicate, _left, _right) {

}

NestedLoopJoin::~NestedLoopJoin() {
	delete left;
	delete right;
}

bool NestedLoopJoin::GetNext(Record& _record) {
	return false;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) : 
	schema(_schema), producer(_producer), om(schema) {
}

DuplicateRemoval::~DuplicateRemoval() {
	delete producer;
}

bool DuplicateRemoval::GetNext(Record& _record) {
	while (producer->GetNext(_record)) {
		_record.SetOrderMaker(&om);
		if (m.IsThere(_record)) {
			continue;
		} else {
			SInt zero(0);
			Record r_copy; 
			r_copy = _record;
			m.Insert(r_copy, zero);
			return true;
		}
	}
	return false;
}

ostream& DuplicateRemoval::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "DISTINCT\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	return _os;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) :
	schemaIn(_schemaIn), schemaOut(_schemaOut), compute(_compute),
	producer(_producer) {

}

Sum::~Sum() {
	delete producer;
}

bool Sum::GetNext(Record& _record) {
	if (done) return false;
	int intRes = 0;
	double doubleRes = 0.;
	Type t;
	while(producer->GetNext(_record)){
		int nextInt = 0;
		double nextDouble = 0;
		t = compute.Apply(_record, nextInt, nextDouble);
		intRes += nextInt;
		doubleRes += nextDouble;
	}

	char* res;
	int totSpace = sizeof (int) * 2;
	if (t == Integer) {
		totSpace += sizeof(int);
	} else {
		totSpace += sizeof(double);
	}

	char* space = new char[totSpace];
	*((int*) &(space[0])) = totSpace;
	*((int*) &(space[4])) = sizeof(int)*2;

	if (t == Integer) {
		*((int*) &(space[8])) = intRes;
	}
	else{
		*((double*) &(space[8])) = doubleRes;
	}

	_record.CopyBits(space, totSpace);

	done = true;
	return true;
}

ostream& Sum::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "SUM\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	 return _os;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) :
	schemaIn(_schemaIn), schemaOut(_schemaOut), groupingAtts(_groupingAtts),
	compute(_compute), producer(_producer) {

}

GroupBy::~GroupBy() {
	delete producer;
}

bool GroupBy::GetNext(Record& _record) {
	// on first call
	// fetch all records
	// for each record
		// set the ordermaker for the record
		// if the record isn't in the map
			// insert the record into the map, applying the function to the record and storing the result as the value
		// if it is
			// apply the function to the record and update the corresponding value in the map

	// on subsequent calls
	// pull current entry from map
	// generate a record representing the sum result (like in Sum::GetNext)
	// generate another record by projecting the original record to the grouping atts
	// append the two records
	// advance the map pointer
	// return the frankenrecord
	return false;
}

ostream& GroupBy::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "GROUP BY " << groupingAtts << "\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	return _os;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) :
	schema(_schema), outFile(_outFile), producer(_producer) {

}

WriteOut::~WriteOut() {
	delete producer;
}

bool WriteOut::GetNext(Record& _record) {
	// create output text file
	ofstream f(outFile, ios::out | ios::trunc);
	if (!f.is_open()) {
		cerr << "Couldn't open output file " << outFile << endl;
	}
	f << schema << '\n';
	while (producer->GetNext(_record)) {
		// append record to file
		_record.print(f, schema);
		f << '\n';
	}

	return false;
}

ostream& WriteOut::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "OUTPUT (" << schema.GetNoTuples() << " tuples, "<< schema.GetNumAtts() << " atts)\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	return _os;
}

void QueryExecutionTree::ExecuteQuery() {
	Record r;
	root->GetNext(r);
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	_os << "QUERY EXECUTION TREE\n";
	_op.root->print(_os, 0);
	return _os;
}

// QueryExecutionTree::~QueryExecutionTree() {
// 	delete root;
// }