#include <iostream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file, string _tblName) {
}

Scan::~Scan() {

}

bool Scan::GetNext(Record& _record) {
	return false;
}

ostream& Scan::print(ostream& _os) {
	_os << "SCAN " << tblName << " {" << schema << "}";
	return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
}

Select::~Select() {

}

bool Select::GetNext(Record& _record) {
	return false;	
}

ostream& Select::print(ostream& _os) {
	_os << "SELECT {" << schema << "} {" << predicate << "}";
	return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

}

Project::~Project() {

}

bool Project::GetNext(Record& _record) {
	return false;
}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {

}

Join::~Join() {

}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN";
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


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {

}

DuplicateRemoval::~DuplicateRemoval() {

}

bool DuplicateRemoval::GetNext(Record& _record) {
	return false;
}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT";
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

}

Sum::~Sum() {

}

bool Sum::GetNext(Record& _record) {
	return false;
}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

}

GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record) {
	return false;
}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {

}

WriteOut::~WriteOut() {

}

bool WriteOut::GetNext(Record& _record) {
	return false;
}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT";
}


void QueryExecutionTree::ExecuteQuery() {
}

ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
