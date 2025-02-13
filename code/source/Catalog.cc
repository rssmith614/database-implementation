#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

static int readTables(void* data, int argc, char** argv, char** azColName) {
	string tName = argv[0];
	int noTuples = atoi(argv[1]);
	string path = argv[2];

	// each entry looks like {tName: {noTuples, path}}
	static_cast<map<string, pair<int, string> >*>(data)->insert(make_pair(tName, make_pair(noTuples, path)));

	return 0;
} 

static int readAttributes(void* data, int argc, char** argv, char** azColName) {
	StringVector attributes;
	StringVector types;
	IntVector distincts;
	string curTable("");

	SString aName = (string) argv[0];
	int aPos = atoi(argv[1]);
	SString aType = (string) argv[2];
	SInt noDistinct = atoi(argv[3]);
	string tName = argv[4];

	// cout << argv[0] << " " << argv[1] << " " << argv[2] << " " << argv[3] << " " << argv[4] << " " << endl;

	attributes.Append(aName);
	types.Append(aType);
	distincts.Append(noDistinct);
	// temporary schema with the current attribute
	Schema curSchema = Schema(attributes, types, distincts);

	// check if we already have a schema for the table
	auto get = static_cast<unordered_map<string, Schema>*>(data)->find(tName);
	if (get != static_cast<unordered_map<string, Schema>*>(data)->end()) {
		// query returns attributes in positional order, so appending is fine
		get->second.Append(curSchema);
	} else {
		// table doesn't have a schema defined yet
		static_cast<unordered_map<string, Schema>*>(data)->insert(make_pair(tName, curSchema));
	}

	return 0;
}

static int readIndexes(void* data, int argc, char** argv, char** azColName) {
	StringVector attributes;
	StringVector types;
	IntVector distincts;
	string curTable("");

	string idxName = argv[0];
	SString aName = (string) argv[1];
	string tName = argv[2];

	// static_cast<Map<SString, SString>*>(data)->Insert(aName, idxName);

	static_cast<vector<pair<SString, pair<string, string> > >*>(data)->push_back(make_pair(aName, make_pair(tName, idxName)));
	
	return 0;
}

Catalog::Catalog(SString& _fileName) {
	filename = _fileName;
	int rc = sqlite3_open(((string) _fileName).c_str(), &(db));
	if (rc != SQLITE_OK) {
		cout << "unable to open database " << _fileName << endl;
	}

	string stmt = "CREATE TABLE IF NOT EXISTS Tables (name VARCHAR, noTuples INT, path VARCHAR);";
	char* errMsg;
	rc = sqlite3_exec(db, stmt.c_str(), NULL, 0, &errMsg);

	if (rc != SQLITE_OK) {
		cerr << "Error in \'Tables\' table creation" << endl;
		sqlite3_free(errMsg);
	}

	stmt = "CREATE TABLE IF NOT EXISTS Attributes (name VARCHAR, position INT, type VARCHAR, noDistinct INT, tablename VARCHAR);";
	rc = sqlite3_exec(db, stmt.c_str(), NULL, 0, &errMsg);

	if (rc != SQLITE_OK) {
		cerr << "Error in \'Attributes\' table creation" << endl;
		sqlite3_free(errMsg);
	}

	stmt = "CREATE TABLE IF NOT EXISTS Indexes (name VARCHAR, attribute VARCHAR, tablename VARCHAR);";
	rc = sqlite3_exec(db, stmt.c_str(), NULL, 0, &errMsg);

	if (rc != SQLITE_OK) {
		cerr << "Error in \'Indexes\' table creation" << endl;
		sqlite3_free(errMsg);
	}

	stmt = "SELECT * FROM Tables;";
	rc = sqlite3_exec(db, stmt.c_str(), readTables, (void*)&(table_map), &errMsg);

	if (rc != SQLITE_OK) {
		sqlite3_free(errMsg);
	}

	stmt = "SELECT * FROM Attributes ORDER BY position;";
	rc = sqlite3_exec(db, stmt.c_str(), readAttributes, (void*)&(schema_map), &errMsg);

	if (rc != SQLITE_OK) {
		cerr << errMsg << endl;
		sqlite3_free(errMsg);
	}

	stmt = "SELECT * FROM Indexes;";
	rc = sqlite3_exec(db, stmt.c_str(), readIndexes, (void*)&(index_map), &errMsg);

	if (rc != SQLITE_OK) {
		cerr << errMsg << endl;
		sqlite3_free(errMsg);
	}
}

Catalog::~Catalog() {
	Catalog::Save();
	sqlite3_close(db);
}

bool Catalog::Save() {

	if (!dirty) {
		return false;
	}
	
	string stmt;
	int rc;
	char* errMsg;
	stmt = "DELETE FROM Tables";
	rc = sqlite3_exec(db, stmt.c_str(), readTables, (void*)&(table_map), &errMsg);

	if (rc != SQLITE_OK) {
		sqlite3_free(errMsg);
	}

	stmt = "DELETE FROM Attributes;";
	rc = sqlite3_exec(db, stmt.c_str(), readAttributes, (void*)&(schema_map), &errMsg);
	
	if (rc != SQLITE_OK) {
		cerr << errMsg << endl;
		sqlite3_free(errMsg);
	}

	stmt = "DELETE FROM Indexes;";
	rc = sqlite3_exec(db, stmt.c_str(), readIndexes, (void*)&(index_map), &errMsg);
	
	if (rc != SQLITE_OK) {
		cerr << errMsg << endl;
		sqlite3_free(errMsg);
	}

	for (auto it1 = table_map.begin(); it1 != table_map.end(); it1++) {
		stmt = "INSERT INTO Tables VALUES(?, ?, ?);";
		rc = sqlite3_prepare_v2(db, stmt.c_str(), -1, &stmt_handle, &stmt_leftover);
		if (rc != SQLITE_OK){
			cout << "not ok" << stmt << endl;
			cout << "error is" << sqlite3_errmsg(db) << endl;
			exit(1);
		}
		sqlite3_reset(stmt_handle);
		sqlite3_clear_bindings(stmt_handle);
		sqlite3_bind_text(stmt_handle, 1, it1->first.c_str(), -1, 0);
		sqlite3_bind_int(stmt_handle, 2, it1->second.first);
		sqlite3_bind_text(stmt_handle, 3, it1->second.second.c_str(), -1, 0);
		rc = sqlite3_step(stmt_handle);
		if (rc != SQLITE_DONE){
			cout << "not done" << stmt << endl;
			cout << "error is" << sqlite3_errmsg(db) << endl;
			exit(1);
		}
		sqlite3_finalize(stmt_handle);

		const char type_strings[4][10] = {"Integer", "Float", "String", "Name"};
		auto get = schema_map.find(it1->first);
		Schema schema = get->second;
		AttributeVector& atts = schema.GetAtts();
		for(int i = 0; i < schema.GetNumAtts(); i++){
			stmt = "INSERT INTO Attributes VALUES(?, ?, ?, ?, ?);";
			rc = sqlite3_prepare_v2(db, stmt.c_str(), -1, &stmt_handle, &stmt_leftover);
			if (rc != SQLITE_OK){
				cout << "not ok" << stmt << endl;
				cout << "error is" << sqlite3_errmsg(db) << endl;
				exit(1);
			}
			string att_type = type_strings[atts[i].type];
			sqlite3_reset(stmt_handle);
			sqlite3_clear_bindings(stmt_handle);
			sqlite3_bind_text(stmt_handle, 1, ((string)atts[i].name).c_str(), -1, 0);
			sqlite3_bind_int(stmt_handle, 2, i);
			sqlite3_bind_text(stmt_handle, 3, att_type.c_str(), -1, 0);
			sqlite3_bind_int(stmt_handle, 4, (int)atts[i].noDistinct);
			sqlite3_bind_text(stmt_handle, 5, it1->first.c_str(), -1, 0);
			rc = sqlite3_step(stmt_handle);
			if (rc != SQLITE_DONE){
				cout << "not done" << stmt << endl;
				cout << "error is" << sqlite3_errmsg(db) << endl;
				exit(1);
			}
			sqlite3_finalize(stmt_handle);			
		}

	}

	for (auto index : index_map) {
		stmt = "INSERT INTO Indexes VALUES(?, ?, ?);";
		rc = sqlite3_prepare_v2(db, stmt.c_str(), -1, &stmt_handle, &stmt_leftover);
		if (rc != SQLITE_OK){
			cout << "not ok" << stmt << endl;
			cout << "error is" << sqlite3_errmsg(db) << endl;
			exit(1);
		}
		sqlite3_reset(stmt_handle);
		sqlite3_clear_bindings(stmt_handle);
		sqlite3_bind_text(stmt_handle, 1, (index.second.second).c_str(), -1, 0); // index name
		sqlite3_bind_text(stmt_handle, 2, ((string) index.first).c_str(), -1, 0); // attribute name
		sqlite3_bind_text(stmt_handle, 3, (index.second.first).c_str(), -1, 0); // table name
		rc = sqlite3_step(stmt_handle);
		if (rc != SQLITE_DONE){
			cout << "not done" << stmt << endl;
			cout << "error is" << sqlite3_errmsg(db) << endl;
			exit(1);
		}
		sqlite3_finalize(stmt_handle);
	}

	return true;
}

bool Catalog::GetNoTuples(SString& _table, SInt& _noTuples) {

	// ensure table exists
	auto get = table_map.find(_table);
	if (get != table_map.end()) {
		// recall {name: {noTuples, path}} structure
		_noTuples = get->second.first;
	} else {
		cerr << "Error: table " << _table << " not found";
		return false;
	}

	return true;
}

void Catalog::SetNoTuples(SString& _table, SInt& _noTuples) {
	// ensure table exists
	auto get = table_map.find(_table);
	if (get != table_map.end()) {
		// recall {name: {noTuples, path}} structure
		get->second.first = _noTuples;
		dirty = true;
	} else {
		cerr << "Error: table " << _table << " not found";
	}
}

bool Catalog::GetDataFile(SString& _table, SString& _path) {

	// ensure table exists
	auto get = table_map.find(_table);
	if (get != table_map.end()) {
		// recall {name: {noTuples, path}} structure
		_path = get->second.second;
	} else {
		cerr << "Error: table " << _table << " not found";
		return false;
	}

	return true;
}

void Catalog::SetDataFile(SString& _table, SString& _path) {

	// ensure table exists
	auto get = table_map.find(_table);
	if (get != table_map.end()) {
		// recall {name: {noTuples, path}} structure
		get->second.second = _path;
		dirty = true;
	} else {
		cerr << "Error: table " << _table << " not found";
	}

}

bool Catalog::GetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct) {

	auto get = schema_map.find(_table);
	if (get != schema_map.end()) {
		_noDistinct = get->second.GetDistincts(_attribute);
	} else {
		cerr << "Error: table " << _table << " not found" << endl;
		return false;
	}

	return true;
}

void Catalog::SetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct) {
	
	auto get = schema_map.find(_table);
	if (get != schema_map.end()) {
		get->second.SetDistincts(_attribute, _noDistinct);
		dirty = true;
	} else {
		cerr << "Error: table " << _table << " not found" << endl;
	}
}

void Catalog::GetTables(StringVector& _tables) {

	for (auto table : table_map) {
		SString tName(table.first);
		_tables.Append(tName);
	}
}

bool Catalog::GetAttributes(SString& _table, StringVector& _attributes) {

	// attributes are not strings :(
	auto get = schema_map.find(_table);
	if (get != schema_map.end()) {
		// build a StringVector from the names of the table's attributes
		for (int i=0; i < get->second.GetAtts().Length(); i++) {
			SString att(get->second.GetAtts()[i].name);
			_attributes.Append(att);
		}
	} else {
		cerr << "Error: table " << _table << " not found";
		return false;
	}

	return true;
}

bool Catalog::GetSchema(SString& _table, Schema& _schema) {

	// ensure table exists
	auto get = schema_map.find(_table);
	if (get != schema_map.end()) {
		_schema = get->second;
	} else {
		cerr << "Error: table " << _table << " not found" << endl;
		return false;
	}

	return true;
}

bool Catalog::CreateTable(SString& _table, StringVector& _attributes,
	StringVector& _attributeTypes) {

	// check duplicate table name
	auto get = table_map.find(_table);
	if (get != table_map.end()) {
		cerr << "Error: table " << _table << " already exists" << endl;
		return false;
	}

	// check duplicate attribute names
	unordered_set<string> attSet;
	for (int i=0; i < _attributes.Length(); i++) {
		// unordered set find costs O(1)
		auto get = attSet.find(_attributes[i]);
		if (get == attSet.end()) {
			// unordered set insertion costs O(1)
			attSet.insert(_attributes[i]);
		} else {
			cerr << "Error: duplicate attribute " << _attributes[i] << endl;
			return false;
		}
	}

	// check attribute type
	for (int i=0; i < _attributes.Length(); i++) {
		if ((string)_attributeTypes[i] != "INTEGER" &&
			(string)_attributeTypes[i] != "FLOAT" &&
			(string)_attributeTypes[i] != "STRING") {
				cerr << "Error: unrecognized type " << (string)_attributeTypes[i] << " only data types of INTEGER, FLOAT, and STRING are valid" << endl;
				return false;
		}
	}
	
	// save table info in memory
	int noTuples = 0;
	string path = (string) _table + ".dat";
	// {name: {noTuples, path}}
	table_map.insert(make_pair((string) _table, make_pair(noTuples, path)));

	// save schema info in memory
	IntVector numDistinct;
	SInt zero = 0;
	numDistinct.Append(zero);
	Schema newSchema = Schema(_attributes, _attributeTypes, numDistinct);
	schema_map.insert(make_pair((string) _table, newSchema));

	dirty = true;
	return true;
}

bool Catalog::DropTable(SString& _table) {

	// ensure table exists
	auto get = table_map.find(_table);
	if (get != table_map.end()) {
		// remove in-memory data
		table_map.erase(_table);
		schema_map.erase(_table);
	} else {
		cerr << "Error: no such table " << _table << endl;
		return false;
	}

	dirty = true;
	return true;
}

bool Catalog::CreateIndex(SString _attName, string _tableName, string _idxName) {
	if (HasIndex(_attName, _tableName, _idxName)) {
		cout << "Error: An index already exists on attribute " << _attName << endl;
		return false;
	}
	
	index_map.push_back(make_pair(_attName, make_pair(_tableName, _idxName)));

	dirty = true;
	return true;
}

bool Catalog::HasIndex(SString _attName, string &_tableName, string &_idxName) {
	for (auto index : index_map) {
		if (index.first == _attName) {
			_tableName = index.second.first;
			_idxName = index.second.second;
			return true;
		}
	}

	return false;
}

ostream& operator<<(ostream& _os, Catalog& _c) {

	const char type_strings[4][10] = {"Integer", "Float", "String", "Name"};
	for (auto table : _c.table_map) {
		_os << table.first << '\t' << table.second.first << '\t' << table.second.second << '\n';
		auto get = _c.schema_map.find(table.first);
		Schema schema = get->second;
		AttributeVector& atts = schema.GetAtts();
		for (int i=0; i < schema.GetNumAtts(); i++) {
			string att_type = type_strings[atts[i].type];
			_os << "|_\t" << atts[i].name << '\t' << att_type << '\t' << atts[i].noDistinct << '\n';
		}
	}

	return _os;
}
