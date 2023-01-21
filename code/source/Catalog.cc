#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

// Not done
Catalog::Catalog(SString& _fileName) {

}

Catalog::~Catalog() {

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
