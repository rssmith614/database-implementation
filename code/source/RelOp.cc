#include <iostream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
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

ostream& Scan::print(ostream& _os) {
	_os << "SCAN " << tblName << " {" << schema << "}";
	return _os;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) :
	schema(_schema), predicate(_predicate), constants(constants),
	producer(_producer) {

}

Select::~Select() {

}

bool Select::GetNext(Record& _record) {
	while (true) {
		bool ret = producer->GetNext(_record);
		if (false == ret) return false;

		if (true == predicate.Run(_record, constants)) return true;
	}
}

ostream& Select::print(ostream& _os) {
	_os << "SELECT {" << schema << "} {" << predicate << "}";
	return _os;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer):
	schemaIn(_schemaIn), schemaOut(_schemaOut), numAttsInput(_numAttsInput),
	numAttsOutput(_numAttsOutput), keepMe(_keepMe), producer(_producer) {

}

Project::~Project() {

}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) :
	schemaLeft(_schemaLeft), schemaRight(_schemaRight), schemaOut(_schemaOut),
	predicate(_predicate), left(_left), right(_right) {

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


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) : 
	schema(_schema), producer(_producer) {

}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT";
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) :
	schemaIn(_schemaIn), schemaOut(_schemaOut), compute(_compute),
	producer(_producer) {

}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) :
	schemaIn(_schemaIn), schemaOut(_schemaOut), groupingAtts(_groupingAtts),
	compute(_compute), producer(_producer) {

}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) :
	schema(_schema), outFile(_outFile), producer(_producer) {

}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
