#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "Map.h"
#include "File.h"

using namespace std;


class DBFile {
private:
	File file;
	string fileName;

	off_t currentPagePos;
	Page currentPage;

public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	int Create (char* fpath, FileType file_type);
	int Open (char* fpath);
	int Close ();

	void MoveFirst ();
	void SetPage(off_t _destinationPage);
	int GetNext (Record& _fetchMe);

	void AppendRecord (Record& _addMe);
	void Load (Schema& _schema, char* textFile);

	off_t GetCurrentPage();
};



#endif //DBFILE_H
