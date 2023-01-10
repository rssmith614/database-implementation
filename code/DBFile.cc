#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	return 0;
}

int DBFile::Open (char* f_path) {
	return 0;
}

void DBFile::Load (Schema& schema, char* textFile) {
	currentPagePos = 0;
	currentPage.EmptyItOut();

	FILE* f = fopen(textFile, "rt");
	Record rec;
	while (rec.ExtractNextRecord (schema, *f)) {
		AppendRecord(rec);
	}
}

int DBFile::Close () {
	return 0;
}

void DBFile::MoveFirst () {
	currentPagePos = 0;
	file.GetPage(currentPage, currentPagePos);
}

void DBFile::AppendRecord (Record& rec) {
	if (0 == currentPage.Append(rec)) {
		// save the current page to File
		// start a new page and add the record to this new page
		// increment currentPagepos
	}
}

int DBFile::GetNext (Record& rec) {
	if (0 == currentPage.GetFirst(rec)) {
		// move to the next page in File
	}

	return 1;
}
