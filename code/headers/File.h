#ifndef _FILE_H
#define _FILE_H

#include <string>

#include "Record.h"
#include "List.cc"

#include <unordered_set>

using namespace std;


class Record;

class Page {
private:
	
	int numRecs;

protected:

	int curSizeInBytes;
	RecordList myRecs;

public:
	// constructor & destructor
	Page();
	virtual ~Page();

	// write records to bits
	void ToBinary(char* bits);

	// extract records from bits
	void FromBinary(char* bits);

	// delete current record from page and return it
	// return 0 if there are no records in the page, something else otherwise
	int GetFirst(Record& firstOne);

	// append record to the end of page
	// return 1 on success and 0 if there is no space on page
	// record is consumed so it has no value after Append is called
	int Append(Record& addMe);

	// empty records from page
	void EmptyItOut();
};

class IndexPage : public Page {
public: enum pageType { INTERMEDIATE, LEAF };
private:

	pageType type = LEAF;

	off_t parent = -1;
	off_t pageId;

	int lastPtr;
	int numKeys;

	vector<int> keys;
	vector<off_t> ptrs;

public:	

	IndexPage();
	~IndexPage();

	off_t Add(int key, off_t ptr);
	int Find(int key, off_t &ptr);
	int FindRange(int lower, int upper, unordered_set<off_t> &ptrs, off_t &sibling);

	int AddIntermediate(int key, off_t ptr);

	void Generate(vector<int> &keys, vector<off_t> &ptrs);
	// remove the upper half of the keys from the page and return them
	void Split(vector<int> &newKeys, vector<off_t> &newPtrs);

	int SetSibling(off_t siblingPtr);
	int GetSibling(off_t &siblingPtr);

	void SetParent(off_t parentPtr);
	off_t GetParent();

	void SetPageNumber(off_t newPageNum, vector<off_t> &affectedChildren);
	void SetPageType(pageType newType);

	int PromoteEnd();

	virtual void ToBinary(char* bits);
	virtual void FromBinary(char* bits);

	void EmptyItOut();

	int GetChildren(vector<off_t> &ptrs);
	void Print(ostream &_os);
};

class File {
private:
	int fileDescriptor;
	string fileName;
	off_t curLength;

public:
	File();
	virtual ~File();
	File(const File& _copyMe);
	File& operator=(const File& _copyMe);

	// return length of file, in pages
	off_t GetLength();

	// open file
	// if length is 0, create new file; existent file is erased
	// return 0 on success, -1 otherwise
	int Open(int length, char* fName);

	// get specified page from file
	// return 0 on success, -1 otherwise
	int GetPage(Page& putItHere, off_t whichPage);
	
	int GetPage(IndexPage& putItHere, off_t whichPage);

	// write page to file
	// if write is past end of file (beyond length in pages), all new pages that
	// are past last page and before page to be written are zeroed out
	void AddPage(Page& addMe, off_t whichPage);

	void AddPage(IndexPage& addMe, off_t whichPage);

	// close file and return length in number of pages
	int Close ();
};

#endif //_FILE_H
