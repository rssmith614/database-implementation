#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

// Not done
Catalog::Catalog(string& _fileName) {
	this->filename = _fileName;
	int rc = sqlite3_open(_fileName.c_str(), &(this->db));
	if (rc != SQLITE_OK) {
		fprintf(stderr, "%s\n", sqlite3_errmsg(this->db));
		sqlite3_close(this->db);
		exit(1);
	}
	string stmt = "CREATE TABLE IF NOT EXISTS catalog (name VARCHAR(255), num_tuples INT, file VARCHAR(255));";
	rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	else {
		rc = sqlite3_step(this->stmt_handle);
	}
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	sqlite3_finalize(this->stmt_handle);
	stmt = "CREATE TABLE IF NOT EXISTS attributes (table_name VARCHAR(255), position INT, name VARCHAR(255), type VARCHAR(255), num_distinct INT);";
	rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	else {
		rc = sqlite3_step(this->stmt_handle);
	}
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	sqlite3_finalize(this->stmt_handle);
	
	// get table names, tuples, and filepaths
	stmt = "SELECT name, num_tuples, file FROM catalog;";
	rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}

	// read tuples from statement handle
	while (true) {
		rc = sqlite3_step(this->stmt_handle);
		if (rc == SQLITE_DONE) break;
		if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
			fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
			break;
		}
		string name = reinterpret_cast<const char*>(sqlite3_column_text(this->stmt_handle, 0));
		int size = sqlite3_column_int(this->stmt_handle, 1);
		string file = reinterpret_cast<const char*>(sqlite3_column_text(this->stmt_handle, 2));

		this->table_names.push_back(name);
		this->num_tuples[name] = size;
		this->files[name] = file;
	}
	sqlite3_finalize(this->stmt_handle);

	// initalize schema information
	for (vector<string>::iterator table = this->table_names.begin(); table != this->table_names.end(); ++table) {
		stmt = "SELECT name, num_distinct, type FROM attributes WHERE table_name = '" + *table + "' ORDER BY position ASC;";
		rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
		if (rc != SQLITE_OK) {
			fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
			continue;
		}

		vector<string> atts;
		vector<string> att_types;
		vector<unsigned int> num_distincts;
		while (true) {
			rc = sqlite3_step(this->stmt_handle);
			if (rc == SQLITE_DONE) break;
			if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
				fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
				break;
			}
			string name = reinterpret_cast<const char*>(sqlite3_column_text(this->stmt_handle, 0));
			int distincts = sqlite3_column_int(this->stmt_handle, 1);
			string t = reinterpret_cast<const char*>(sqlite3_column_text(this->stmt_handle, 2));

			atts.push_back(name);
			num_distincts.push_back(distincts);
			att_types.push_back(t);
		}
		this->schema_map[*table] = Schema(atts, att_types, num_distincts);
		sqlite3_finalize(this->stmt_handle);
	}
	this->dirty = false;
}

Catalog::~Catalog() {
	if (this->dirty) this->Save();
	sqlite3_close(this->db);
}

bool Catalog::Save() {
	string stmt = "DELETE FROM catalog;";
	int rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	else {
		rc = sqlite3_step(this->stmt_handle);
	}
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	sqlite3_finalize(this->stmt_handle);

	stmt = "DELETE FROM attributes;";
	rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
	if (rc != SQLITE_OK) {
		fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	else {
		rc = sqlite3_step(this->stmt_handle);
	}
	if (rc != SQLITE_DONE) {
		fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
	}
	sqlite3_finalize(this->stmt_handle);

	for (vector<string>::iterator table = this->table_names.begin(); table != this->table_names.end(); ++table) {
		sprintf(this->buffer, "INSERT INTO catalog VALUES('%s', %d, '%s');", (*table).c_str(), this->num_tuples[*table], this->files[*table].c_str());
		stmt = this->buffer;
		rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
		if (rc != SQLITE_OK) {
			fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
		}
		else {
			rc = sqlite3_step(this->stmt_handle);
		}
		if (rc != SQLITE_DONE) {
			fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
		}
		sqlite3_finalize(this->stmt_handle);
		vector<Attribute> atts = this->schema_map[*table].GetAtts();
		for (int i = 0; i < atts.size(); ++i) {
			string t;
			switch(atts[i].type) {
				case Integer:
					t = "INTEGER";
					break;
				case Float:
					t = "FLOAT";
					break;
				case String:
					t = "STRING";
					break;
				default:
					t = "UNKNOWN";
			}
			sprintf(this->buffer, "INSERT INTO attributes VALUES('%s', %d, '%s', '%s', %d);", (*table).c_str(), i, atts[i].name.c_str(), t.c_str(), atts[i].noDistinct);
			stmt = this->buffer;
			rc = sqlite3_prepare_v2(this->db, stmt.c_str(), -1, &(this->stmt_handle), &(this->stmt_leftover));
			if (rc != SQLITE_OK) {
				fprintf(stderr, "Cannot compile statment %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
			}
			else {
				rc = sqlite3_step(this->stmt_handle);
			}
			if (rc != SQLITE_DONE) {
				fprintf(stderr, "Cannot run statement %s\n The error is %s\n", stmt.c_str(), sqlite3_errmsg(this->db));
			}
			sqlite3_finalize(this->stmt_handle);
		}
	}

	this->dirty = false;
	return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	if (this->num_tuples.find(_table) == this->num_tuples.end())
		return false;
	_noTuples = this->num_tuples[_table];
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	this->num_tuples[_table] = _noTuples;
	this->dirty = true;
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	if (this->schema_map.find(_table) == this->schema_map.end())
		return false;
	_path = this->files[_table];
	return true;
}

void Catalog::SetDataFile(string& _table, string& _path) {
	this->files[_table] = _path;
	this->dirty = true;
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
	if (this->schema_map.find(_table) == this->schema_map.end()) {
		_noDistinct = 0;
		return false;
	}
	
	_noDistinct = this->schema_map[_table].GetDistincts(_attribute);
	return true;
}

void Catalog::SetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
	this->schema_map[_table].SetDistincts(_attribute, _noDistinct);
	this->dirty = true;
}

void Catalog::GetTables(vector<string>& _tables) {
	_tables = this->table_names;
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	if (this->schema_map.find(_table) == this->schema_map.end())
		return false;
	vector<Attribute> atts = this->schema_map[_table].GetAtts();
	_attributes = vector<string>(atts.size());
	for (int i = 0; i < atts.size(); ++i) {
		_attributes[i] = atts[i].name;
	}
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	if (this->schema_map.find(_table) == this->schema_map.end())
		return false;
	_schema = this->schema_map[_table];
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	if (this->schema_map.find(_table) != schema_map.end())
		return false;
	vector<unsigned int> num_distincts(_attributes.size(), 0);
	this->table_names.push_back(_table);
	this->schema_map[_table] = Schema(_attributes, _attributeTypes, num_distincts);
	this->num_tuples[_table] = 0;
	this->dirty = true;
	return true;
}

bool Catalog::DropTable(string& _table) {
	if (this->schema_map.find(_table) == this->schema_map.end())
		return false;
	for (vector<string>::iterator name = this->table_names.begin(); name != this->table_names.end(); ++name) {
		if (*name == _table) {
			this->table_names.erase(name);
			name--;
		}
	}
	this->schema_map.erase(_table);
	this->num_tuples.erase(_table);
	this->files.erase(_table);
	this->dirty = true;
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	if (_c.dirty) _os << "Catalog has changed since last save." << endl;
	else _os << "No changes since last save" << endl;
	for (vector<string>::iterator name = _c.table_names.begin(); name != _c.table_names.end(); ++name) {
		string table = *name;
		_os << table << '\t' << _c.num_tuples[table] << '\t' << _c.files[table] << '\n';
		_os << _c.schema_map[table] << endl;
	}
	return _os;
}