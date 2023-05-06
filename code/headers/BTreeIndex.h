#ifndef BTREEINDEX_H
#define BTREEINDEX_H

#include "Map.h"
#include "File.h"
#include "DBFile.h"

#include <unordered_set>

class BTreeIndex {
private:

	File idxFile;
	string idxFileName;

    off_t maxPageId = 0;

	off_t currentIdxPagePos;
	IndexPage currentIdxPage;

    DBFile dbFile;

    int attCol;

    bool firstPageWritten = false;

public:
    BTreeIndex();
    ~BTreeIndex();
	BTreeIndex(const BTreeIndex& _copyMe);

    void Read(string fileName);

    int Build(string indexName, string tblName, SString attName, Schema& schema);

    void PrepareNewRoot(int key, off_t left, off_t right);
    void InsertIntermediate(off_t where, int key, off_t ptr);

    int Find(SInt key, off_t &pageNumber);
    int FindRange(SInt lowerBound, SInt upperBound, unordered_set<off_t> &pages);

    void Print(ostream &_os, off_t pageToPrint);

    string GetIdxFileName() {
        return idxFileName;
    }
};

#endif // BTREEINDEX_H