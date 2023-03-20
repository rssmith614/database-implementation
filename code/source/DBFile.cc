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
	fileName = f_path;
	int ret = file.Open(0, (char*) fileName.c_str());
	if (-1 == ret) {
		cerr << "Error opening file " << fileName << endl;
		return 1;
	}
	return 0;
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
	int ret = file.Open(-1, (char*) fileName.c_str());
	if (-1 == ret) {
		cerr << "Error opening file " << fileName << endl;
		return 1;
	}
	currentPagePos = 0;
	ret = file.GetPage(currentPage, currentPagePos);
	if (-1 == ret) {
		cerr << "Error opening file " << fileName << endl;
		return 1;
	}
	return 0;
}

int DBFile::Close () {
	file.AddPage(currentPage, currentPagePos);
	cout << "Generated file with " << file.GetLength() << " pages" << endl;
	file.Close();
	return 0;
}

void DBFile::MoveFirst () {
	currentPagePos = 0;
	file.GetPage(currentPage, currentPagePos);
}

// returns 0 if record was retrieved
int DBFile::GetNext (Record& rec) {
	// look for the next record
	if (0 == currentPage.GetFirst(rec)) {
		// move to the next page
		currentPagePos++;
		// check if there are any pages left
		if (currentPagePos == file.GetLength()) {
			return 1;
		}
		// get the next page in File
		if (0 == file.GetPage(currentPage, currentPagePos)) {
			// look for next record
			if (0 == currentPage.GetFirst(rec)) {
				return 1;
			}
		} else {
			return 1;
		}
	}

	return 0;
}

void DBFile::AppendRecord (Record& rec) {
	// try to append the record to the current page
	if (0 == currentPage.Append(rec)) { /* page is full */
		// save the current page to File
		// start a new page and add the record to this new page
		file.AddPage(currentPage, currentPagePos);
		currentPage.EmptyItOut();
		currentPage.Append(rec);
		// increment currentPagepos
		currentPagePos++;
	}
}

void DBFile::Load (Schema& schema, char* textFile) {
	currentPagePos = 0;
	currentPage.EmptyItOut();

	FILE* f = fopen(textFile, "rt");
	
	Record rec;
	while (rec.ExtractNextRecord (schema, *f)) {
		AppendRecord(rec);
	}

	fclose(f);
}
