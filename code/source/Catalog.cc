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
	static_cast<unordered_map<string, pair<int, string> >*>(data)->insert(make_pair(tName, make_pair(noTuples, path)));

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

Catalog::Catalog(SString& _fileName) {
	this->filename = _fileName;
	int rc = sqlite3_open(((string) _fileName).c_str(), &(this->db));
	if (rc == SQLITE_OK)
		cout << "opened database " << _fileName << endl;

	string stmt = "CREATE TABLE IF NOT EXISTS Tables (name VARCHAR, noTuples INT, path VARCHAR);";
	char* errMsg;
	rc = sqlite3_exec(this->db, stmt.c_str(), NULL, 0, &errMsg);

	if (rc != SQLITE_OK) {
		cerr << "Error in \'Tables\' table creation" << endl;
		sqlite3_free(errMsg);
	}

	stmt = "CREATE TABLE IF NOT EXISTS Attributes (name VARCHAR, position INT, type VARCHAR, noDistinct INT, tablename VARCHAR);";
	rc = sqlite3_exec(this->db, stmt.c_str(), NULL, 0, &errMsg);

	if (rc != SQLITE_OK) {
		cerr << "Error in \'Attributes\' table creation" << endl;
		sqlite3_free(errMsg);
	}

	stmt = "SELECT * FROM Tables;";
	rc = sqlite3_exec(this->db, stmt.c_str(), readTables, (void*)&(this->table_map), &errMsg);

	if (rc != SQLITE_OK) {
		sqlite3_free(errMsg);
	} else {
		// cout << "tables select ok" << endl;
	}

	stmt = "SELECT * FROM Attributes ORDER BY position;";
	rc = sqlite3_exec(this->db, stmt.c_str(), readAttributes, (void*)&(this->schema_map), &errMsg);

	if (rc != SQLITE_OK) {
		cerr << errMsg << endl;
		sqlite3_free(errMsg);
	} else {
		// cout << "attributes select ok" << endl;
	}

	for (auto it = this->table_map.begin(); it != this->table_map.end(); it++) {
		cout << it->first << ": " << it->second.first << ", " << it->second.second << endl;
	}

	for (auto it = this->schema_map.begin(); it != this->schema_map.end(); it++) {
		cout << it->first << ": " << it->second << endl;
	}
}

Catalog::~Catalog() {
	Catalog::Save();
	sqlite3_close(this->db);
}

bool Catalog::Save() {

	return true;
}

bool Catalog::GetNoTuples(SString& _table, SInt& _noTuples) {

	return true;
}

void Catalog::SetNoTuples(SString& _table, SInt& _noTuples) {

}

bool Catalog::GetDataFile(SString& _table, SString& _path) {

	return true;
}

void Catalog::SetDataFile(SString& _table, SString& _path) {

}

bool Catalog::GetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct) {

	return true;
}

void Catalog::SetNoDistinct(SString& _table, SString& _attribute, SInt& _noDistinct) {

}

void Catalog::GetTables(StringVector& _tables) {

}

bool Catalog::GetAttributes(SString& _table, StringVector& _attributes) {

	return true;
}

bool Catalog::GetSchema(SString& _table, Schema& _schema) {

	return true;
}

bool Catalog::CreateTable(SString& _table, StringVector& _attributes,
	StringVector& _attributeTypes) {

	return true;
}

bool Catalog::DropTable(SString& _table) {

	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {

	return _os;
}
