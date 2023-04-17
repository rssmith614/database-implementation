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
	delete left;
	delete right;
}

bool Join::GetNext(Record& _record) {
	return false;
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
	: Join(_schemaLeft, _schemaRight, _schemaOut, _predicate, _left, _right),
	left_om(schemaLeft), right_om(schemaRight) {

	predicate.GetSortOrders(left_om, right_om);

}

NestedLoopJoin::~NestedLoopJoin() {
	
}

bool NestedLoopJoin::GetNext(Record& _record) {
	// on first call
	if (!done) {
		// get all tuples from S (right)
		while (right->GetNext(_record)) {
			_record.SetOrderMaker(&right_om);
			list.Append(_record);
		}

		list.MoveToStart();
		done = true;

		// get a tuple from R
		while (left->GetNext(_record)) {
			hasLRec = true;
			lRec.Swap(_record);
			lRec.SetOrderMaker(&left_om);
			break;
		}
		
		// stop if no tuples in R
		if (!hasLRec)
			return false;
	}

	// loop on R
	while (true) {
		while (!list.AtEnd()) {
			// check if current tuples from R and S join
			if (predicate.Run(lRec, list.Current())) {
				// return joined records
				_record.AppendRecords(lRec, list.Current(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				list.Advance();
				return true;
			}
			list.Advance();
		}
			
		// reset S list
		list.MoveToStart();
		// get a tuple from R
		hasLRec = false;
		while (left->GetNext(_record)) {
			hasLRec = true;
			lRec.Swap(_record);
			lRec.SetOrderMaker(&left_om);
			break;
		}
		
		// stop if R is out of tuples
		if (!hasLRec)
			return false;
	}

	return false;
}

HashJoin::HashJoin(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right)
	: Join(_schemaLeft, _schemaRight, _schemaOut, _predicate, _left, _right),
	left_om(schemaLeft), right_om(schemaRight) {

	predicate.GetSortOrders(left_om, right_om);

}

HashJoin::~HashJoin() {

}

bool HashJoin::GetNext(Record& _record) {
	// on first call
	if (!done) {
		// get all records from S
		while (right->GetNext(_record)) {
			_record.SetOrderMaker(&right_om);
			SInt zero(0);
			map.Insert(_record, zero);
		}

		done = true;

		// get a record from R
		while (left->GetNext(_record)) {
			// search for corresponding tuple in S
			_record.SetOrderMaker(&left_om);
			if (map.IsThere(_record)) {
				hasLRec = true;
				lRec.Swap(_record);
				// append the records from R and S and return
				_record.AppendRecords(lRec, map.CurrentKey(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				map.Advance();
				return true;
			}
		}

		if (!hasLRec)
			return false;
	}

	// loop on R
	while (true) {
		// check if we advanced the map to another matching record
		if (!map.AtEnd()) {
			if (predicate.Run(lRec, map.CurrentKey())) {
				// append the records from R and S and return
				_record.AppendRecords(lRec, map.CurrentKey(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				map.Advance();
				return true;
			}
		}

		// find the next record from R
		hasLRec = false;
		while (left->GetNext(_record)) {
			_record.SetOrderMaker(&left_om);
			// look for its joining tuple in S
			if (map.IsThere(_record)) {
				hasLRec = true;
				lRec.Swap(_record);
				_record.AppendRecords(lRec, map.CurrentKey(), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
				map.Advance();
				return true;
			}
		}

		if (!hasLRec)
			return false;
	}
}

SymmetricHashJoin::SymmetricHashJoin(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right)
	: Join(_schemaLeft, _schemaRight, _schemaOut, _predicate, _left, _right),
	left_om(schemaLeft), right_om(schemaRight), zero(0) {

	predicate.GetSortOrders(left_om, right_om);

}

SymmetricHashJoin::~SymmetricHashJoin() {
	
}

bool SymmetricHashJoin::GetNext(Record& _record) {
	// if there are no records left
	if (R_done && S_done && buffer.Length() == 0)
		return false;

	// if there are records to return
	if (buffer.Length() > 0) {
		buffer.Remove(_record);
		return true;
	}

	// find some records to join
	while (true) {
		// currently working on right (S)
		if (whichSide == S) {
			// if S has records
			if (!S_done && right->GetNext(_record)) {
				// prepare for comparisons
				S_rec.Swap(_record);
				S_rec.SetOrderMaker(&right_om);
				
				// look for record in R_map
				if (R_map.IsThere(S_rec)) {
					// prepare for comparisons
					R_rec.CopyBits(R_map.CurrentKey().GetBits(), R_map.CurrentKey().GetSize());
					R_rec.SetOrderMaker(&left_om);
					// find all hits
					while (!R_map.AtEnd() && predicate.Run(R_rec, S_rec)) {
						// put joined record in buffer
						_record.AppendRecords(R_rec, S_rec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						buffer.Append(_record);
						// we need to check hash collisions
						R_map.Advance();
						R_rec.CopyBits(R_map.CurrentKey().GetBits(), R_map.CurrentKey().GetSize());
						R_rec.SetOrderMaker(&left_om);
					}
					buffer.MoveToStart();
				}

				// insert current record from S into map
				S_map.Insert(S_rec, zero);
			} else {
				S_done = true;
			}
			// switch to left for next loop (R)
			whichSide = R;
		} 
		// currently working on left (R)
		else if (whichSide == R) {
			// if R has records
			if (!R_done && left->GetNext(_record)) {
				// prepare for comparisons
				R_rec.Swap(_record);
				R_rec.SetOrderMaker(&left_om);
				
				// look for record in S_map
				if (S_map.IsThere(R_rec)) {
					// prepare for comparisons
					S_rec.CopyBits(S_map.CurrentKey().GetBits(), S_map.CurrentKey().GetSize());
					S_rec.SetOrderMaker(&right_om);
					// find all hits
					while (!S_map.AtEnd() && predicate.Run(R_rec, S_rec)) {
						// put joined record in buffer
						_record.AppendRecords(R_rec, S_rec, schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());
						buffer.Append(_record);
						// check hash collisions
						S_map.Advance();
						S_rec.CopyBits(S_map.CurrentKey().GetBits(), S_map.CurrentKey().GetSize());
						S_rec.SetOrderMaker(&right_om);
					}
					buffer.MoveToStart();
				}

				// put record from R into map
				R_map.Insert(R_rec, zero);
			} else {
				R_done = true;
			}
			// switch to right for next loop (S)
			whichSide = S;
		}

		// if no records left to return
		if (R_done && S_done && buffer.Length() == 0)
			return false;
		
		// if there are records to return
		if (buffer.Length() > 0) {
			buffer.Remove(_record);
			return true;
		}
	}	
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
	if (!done) {
		Type t;
		// fetch all records
		while (producer->GetNext(_record)) {
			Record copy;
			copy = _record;
			// we only care about unique instances of the grouping attributes,
			// so project everything else out for map insertion
			_record.Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());
			int* keepMe = new int[groupingAtts.numAtts];
			for (int i=0; i<groupingAtts.numAtts; i++) {
				keepMe[i] = i;
			}
			// new record format means new ordermaker
			OrderMaker om(schemaOut, keepMe, groupingAtts.numAtts);
			_record.SetOrderMaker(&om);
			SDouble curSum = 0;
			int ret = m.Find(_record, curSum);
			int nextInt = 0;
			double nextDouble = 0;
			// function gets applied to the original record, since its
			// function attributes haven't been projected away
			t = compute.Apply(copy, nextInt, nextDouble);
			if (t == Integer) {
				curSum = curSum + nextInt;
			} else {
				curSum = curSum + nextDouble;
			}
			if (0 == ret) {
				// save the grouping attributes with the result of the function on the one record
				m.Insert(_record, curSum);
			} else {
				// update the map with the new sum
				Record removedKey;
				SDouble removedData;
				m.Remove(_record, removedKey, removedData);
				m.Insert(removedKey, curSum);
			}
			
		}
		done = true;
		m.MoveToStart();
		// then fall through
	}

	// on subsequent calls
	if (m.AtEnd()) {
		return false;
	}
	// pull current entry from map
	Record groupRecord;
	groupRecord = m.CurrentKey();
	SDouble sum;
	sum = m.CurrentData();

	// generate a record representing the sum result (like in Sum::GetNext)
	// 						metadata			payload
	int totSpace = sizeof(int) + sizeof(int) + sizeof(double);
	char* space = new char[totSpace];
	// metadata
	*((int*) &(space[0])) = totSpace;
	*((int*) &(space[sizeof(int)])) = sizeof(int)*2;
	// payload
	*((double*) &(space[sizeof(int)*2])) = (double) sum;

	Record sumRecord;
	sumRecord.CopyBits(space, totSpace);

	// append the two records
	_record.AppendRecords(sumRecord, groupRecord, 1, groupingAtts.numAtts);
	// advance the map pointer
	m.Advance();
	// return the frankenrecord
	return true;
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
	// f << schema << '\n';
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