#include <iostream>
#include <stack>
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

bool Scan::GetNext(Record& _record) {
	int ret = file.GetNext(_record);
	if (1 == ret) return true;

	return false;
}

ostream& Scan::print(ostream& _os, int depth) {
	_os << "SCAN " << schema.GetNoTuples() << ' ' << tblName;
	return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer)
	// schema(_schema), predicate(_predicate), constants(constants),
	// producer(_producer) 
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
	while (true) {
		bool ret = producer->GetNext(_record);
		if (false == ret) return false;

		if (true == predicate.Run(_record, constants)) return true;
	}
}

ostream& Select::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "SELECT " << predicate << "\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer):
	schemaIn(_schemaIn), schemaOut(_schemaOut), numAttsInput(_numAttsInput),
	numAttsOutput(_numAttsOutput), keepMe(_keepMe), producer(_producer) {

}

Project::~Project() {

}

ostream& Project::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "PROJECT " << schemaOut.GetNoTuples() << "\n" << tabs << "└────>";
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
	_os << "JOIN " << schemaOut.GetNoTuples() << "\n" << tabs << "├────>";
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
}

bool NestedLoopJoin::GetNext(Record& _record) {
	return false;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) : 
	schema(_schema), producer(_producer) {

}

DuplicateRemoval::~DuplicateRemoval() {

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

ostream& WriteOut::print(ostream& _os, int depth) {
	string tabs(depth, '\t');
	_os << "OUTPUT " << schema.GetNoTuples() << "\n" << tabs << "└────>";
	producer->print(_os, depth+1);
	return _os;
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	_os << "QUERY EXECUTION TREE\n";
	_op.root->print(_os, 0);
	return _os;
}

QueryExecutionTree::~QueryExecutionTree() {
	delete root;
}