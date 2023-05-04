#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <string>

#include "Config.h"
#include "Record.h"
#include "File.h"

using namespace std;


Page :: Page() : curSizeInBytes(sizeof (int)), numRecs(0) {
}

Page :: ~Page() {
}

void Page :: EmptyItOut() {
	// get rid of all of the records
	RecordList aux; aux.Swap(myRecs);

	// reset the page size
	curSizeInBytes = sizeof(int);
	numRecs = 0;
}

int Page :: GetFirst(Record& firstOne) {
	// move to the first record
	myRecs.MoveToStart ();

	// make sure there is data 
	if (myRecs.AtEnd()) return 0;

	// and remove it
	myRecs.Remove (firstOne);
	numRecs--;

	char* b = firstOne.GetBits();
	curSizeInBytes -= ((int*)b)[0];

	return 1;
}

int Page :: Append (Record& addMe) {
	char* b = addMe.GetBits();

	// first see if we can fit the record
	if (curSizeInBytes + ((int *) b)[0] > PAGE_SIZE) return 0;

	curSizeInBytes += ((int *) b)[0];
	myRecs.Append(addMe);
	numRecs++;

	return 1;	
}

void Page :: ToBinary (char* bits) {
	// first write the number of records on the page
	((int *) bits)[0] = numRecs;

	char* curPos = bits + sizeof (int);

	// and copy the records one-by-one
	for (myRecs.MoveToStart(); !myRecs.AtEnd(); myRecs.Advance()) {
		char* b = myRecs.Current().GetBits();
		
		// copy over the bits of the current record
		memcpy (curPos, b, ((int *) b)[0]);
		curPos += ((int *) b)[0];
	}
}

void Page :: FromBinary (char* bits) {
	// first read the number of records on the page
	numRecs = ((int *) bits)[0];

	// and now get the binary representations of each
	char* curPos = bits + sizeof (int);

	// first, empty out the list of current records
	RecordList aux; aux.Swap(myRecs);

	// now loop through and re-populate it
	Record temp;
	curSizeInBytes = sizeof (int);
	for (int i = 0; i < numRecs; i++) {
		// get the length of the current record
		int len = ((int *) curPos)[0];
		curSizeInBytes += len;

		// create the record
		temp.CopyBits(curPos, len);

		// add it
		myRecs.Append(temp);
		curPos += len;
	}
}

IndexPage::IndexPage () {
	pageId = 0;
	numKeys = 6000;

	curSizeInBytes = sizeof(pageId);
}

IndexPage::~IndexPage () {
}

void IndexPage :: EmptyItOut() {
	keys.erase(keys.begin(), keys.end());
	ptrs.erase(ptrs.begin(), ptrs.end());
}

off_t IndexPage::Add(int _key, off_t _ptr) {
	// add not successful since current page is not a leaf
	if (type == INTERMEDIATE) {
		auto it = upper_bound(keys.begin(), keys.end(), _key);
		// return index to next node to search
		return ptrs[it - keys.begin()];
	}

	auto key_it = upper_bound(keys.begin(), keys.end(), _key);
	auto ptr_it = ptrs.begin() + (key_it - keys.begin());
	keys.insert(key_it, _key);
	ptrs.insert(ptr_it, _ptr);

	if (sizeof(off_t)*2 + sizeof(int)*2 + sizeof(char) + keys.size() * sizeof(int) + ptrs.size() * sizeof(off_t) > PAGE_SIZE) {
		return -1;
	}

	return 0;
}

int IndexPage::AddIntermediate(int _key, off_t _ptr) {
	auto key_it = upper_bound(keys.begin(), keys.end(), _key);
	auto ptr_it = ptrs.begin() + (key_it - keys.begin()) + 1;
	
	keys.insert(key_it, _key);
	if (key_it != keys.end()) {
		ptrs.insert(ptr_it, _ptr);
	} else {
		ptrs.push_back(_ptr);
	}

	if (sizeof(off_t)*2 + sizeof(int)*2 + sizeof(char) + keys.size() * sizeof(int) + ptrs.size() * sizeof(off_t) > PAGE_SIZE) {
		return -1;
	}

	return 0;
}

int IndexPage::Find(int _key, off_t &_ptr) {
	auto key_it = lower_bound(keys.begin(), keys.end(), _key);

	_ptr = ptrs[key_it - keys.begin()];

	if (type == INTERMEDIATE) {
		// _ptr points to another idxfile page
		return 1;
	} else if (type == LEAF) {
		// _ptr points to dbfile page
		if (*key_it == _key)
			return 0;
		else
			return -1;
	}

	return -1;
}

void IndexPage :: Generate (vector<int> &_keys, vector<off_t> &_ptrs) {
	keys.erase(keys.begin(), keys.end());
	ptrs.erase(ptrs.begin(), ptrs.end());

	keys.insert(keys.begin(), _keys.begin(), _keys.end());
	ptrs.insert(ptrs.begin(), _ptrs.begin(), _ptrs.end());
}

void IndexPage :: Split (vector<int> &newKeys, vector<off_t> &newPtrs) {
	// half
	int m = keys.size() / 2 + 1;

	// move upper values
	newKeys.reserve(m); newPtrs.reserve(m);

	newKeys.insert(newKeys.begin(), keys.begin() + m, keys.end());
	newPtrs.insert(newPtrs.begin(), ptrs.begin() + m, ptrs.end());

	// remove upper values
	keys.resize(m); ptrs.resize(m);
}

int IndexPage :: SetSibling (off_t siblingPtr) {
	if (type == LEAF) {
		ptrs.push_back(siblingPtr);
		return 0;
	} else {
		return -1;
	}
}

int IndexPage :: GetSibling (off_t &siblingPtr) {
	bool doesPageHaveSibling = (keys.size() == ptrs.size() - 1);
	if (type == LEAF && doesPageHaveSibling) {
		siblingPtr = ptrs.back();
		return 0;
	} else {
		return -1;
	}
}

void IndexPage :: SetParent(off_t parentPtr) {
	parent = parentPtr;
}

off_t IndexPage :: GetParent() {
	return parent;
}

void IndexPage :: SetPageNumber(off_t newPageNum, vector<off_t> &affectedChildren) {
	pageId = newPageNum;
	if (type == INTERMEDIATE)
		affectedChildren = ptrs;
}

void IndexPage :: SetPageType(pageType newType) {
	type = newType;
}

int IndexPage :: PromoteEnd() {
	int key = keys.back();
	keys.pop_back();
	return key;
}

void IndexPage :: ToBinary (char* bits) {
	((off_t*) bits)[0] = pageId;
	char* curPos = bits + sizeof(off_t);
	// page size
	((int *) curPos)[0] = keys.size();
	((int *) curPos)[1] = ptrs.size();

	curPos += sizeof(int)*2;

	((off_t*) curPos)[0] = parent;
	curPos += sizeof(off_t);

	char curPageType = (type == LEAF ? 'L' : 'I');

	curPos[0] = curPageType;
	curPos += sizeof(char);

	for (int i=0; i < keys.size(); i++) {
		((int*) curPos)[0] = keys[i];
		curPos += sizeof(int);
	}

	for (int i=0; i < ptrs.size(); i++) {
		((off_t*) curPos)[0] = ptrs[i];
		curPos += sizeof(off_t);
	}
}

void IndexPage :: FromBinary (char* bits) {
	pageId = ((off_t*) bits)[0];
	char* curPos = bits + sizeof(off_t);

	numKeys = ((int*) curPos)[0];
	int numPtrs = ((int*) curPos)[1];

	curPos += sizeof(int)*2;

	parent = ((off_t*) curPos)[0];
	curPos += sizeof(off_t);

	type = (curPos)[0] == 'L' ? LEAF : INTERMEDIATE;
	curPos += sizeof(char);

	keys.erase(keys.begin(), keys.end());
	ptrs.erase(ptrs.begin(), ptrs.end());

	for (int i=0; i < numKeys; i++) {
		int key_b = 0;

		memcpy (&key_b, (int*) curPos, sizeof(int));
		curPos += sizeof(int);

		keys.push_back(key_b);
	}

	for (int i=0; i < numPtrs; i++) {
		off_t ptr_b = 0;

		memcpy (&ptr_b, (off_t*) curPos, sizeof(off_t));
		curPos += sizeof(off_t);

		ptrs.push_back(ptr_b);
	}
}

int IndexPage :: GetChildren (vector<off_t> &_ptrs) {
	if (type == LEAF) {
		return 1;
	} else {
		_ptrs = ptrs;
		return 0;
	}
}

void IndexPage :: Print (ostream &_os) {
	_os << "Page: " << pageId << " (" << (type == 0 ? "Intermediate" : "Leaf") << ")" << '\n';
	_os << "Parent: " << parent << '\n';
	_os << " ";
	for (auto key : keys) {
		_os << key << "\t  ";
	}
	_os << '\n';
	for (auto ptr : ptrs) {
		_os << ptr << "\t";
	}
	_os << '\n';
	_os << "================\n";
}




File :: File () : fileDescriptor(-1), fileName(""), curLength(0) {
}

File :: ~File () {
}

File::File(const File& _copyMe) : fileDescriptor(_copyMe.fileDescriptor),
	fileName(_copyMe.fileName), curLength(_copyMe.curLength) {}

File& File::operator=(const File& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	fileDescriptor = _copyMe.fileDescriptor;
	fileName = _copyMe.fileName;
	curLength = _copyMe.curLength;

	return *this;
}

int File :: Open (int fileLen, char* fName) {
	// figure out the flags for the system open call
	int mode;
	if (fileLen == 0) mode = O_TRUNC | O_RDWR | O_CREAT;
	else mode = O_RDWR;

	// actually do the open
	fileName = fName;
	fileDescriptor = open (fName, mode, S_IRUSR | S_IWUSR);

	// see if there was an error
	if (fileDescriptor < 0) {
		cerr << "ERROR: Open file did not work for " << fileName << "!" << endl;
		return -1;
	}

	// read in the buffer if needed
	if (fileLen != 0) {
		// read in the first few bits, which is the number of pages
		lseek (fileDescriptor, 0, SEEK_SET);	// jump to location in file
		read (fileDescriptor, &curLength, sizeof (off_t));
	}
	else curLength = 0;

	return 0;
}

int File :: Close () {
	// write out the current length in pages
	lseek (fileDescriptor, 0, SEEK_SET);
	write (fileDescriptor, &curLength, sizeof (off_t));

	// close the file
	close (fileDescriptor);

	// and return the size
	return curLength;
}

int File :: GetPage (Page& putItHere, off_t whichPage) {
	if (whichPage >= curLength) {
		cerr << "ERROR: Read past end of the file " << fileName << ": ";
		cerr << "page = " << whichPage << " length = " << curLength << endl;
		return -1;
	}

	// this is because the first page has no data
	whichPage++;

	// read in the specified page
	char* bits = new char[PAGE_SIZE];
	
	lseek (fileDescriptor, PAGE_SIZE * whichPage, SEEK_SET);
	read (fileDescriptor, bits, PAGE_SIZE);
	putItHere.FromBinary(bits);

	delete [] bits;

	return 0;
}

int File :: GetPage (IndexPage& putItHere, off_t whichPage) {
	if (whichPage >= curLength) {
		cerr << "ERROR: Read past end of the file " << fileName << ": ";
		cerr << "page = " << whichPage << " length = " << curLength << endl;
		return -1;
	}

	// this is because the first page has no data
	whichPage++;

	// read in the specified page
	char* bits = new char[PAGE_SIZE];
	
	lseek (fileDescriptor, PAGE_SIZE * whichPage, SEEK_SET);
	read (fileDescriptor, bits, PAGE_SIZE);
	putItHere.FromBinary(bits);

	delete [] bits;

	return 0;
}

void File :: AddPage (Page& addMe, off_t whichPage) {
	// do the zeroing
	for (off_t i = curLength; i < whichPage; i++) {
		char zero[PAGE_SIZE]; bzero(zero, PAGE_SIZE);
		lseek (fileDescriptor, PAGE_SIZE * (i+1), SEEK_SET);
		write (fileDescriptor, &zero, PAGE_SIZE);
	}

	// now write the page
	char* bits = new char[PAGE_SIZE];

	addMe.ToBinary(bits);
	lseek (fileDescriptor, PAGE_SIZE * (whichPage+1), SEEK_SET);
	write (fileDescriptor, bits, PAGE_SIZE);

	curLength = whichPage+1;

	delete [] bits;
}

void File :: AddPage (IndexPage& addMe, off_t whichPage) {
	// do the zeroing
	for (off_t i = curLength; i < whichPage; i++) {
		char zero[PAGE_SIZE]; bzero(zero, PAGE_SIZE);
		lseek (fileDescriptor, PAGE_SIZE * (i+1), SEEK_SET);
		write (fileDescriptor, &zero, PAGE_SIZE);
	}

	// now write the page
	char* bits = new char[PAGE_SIZE];

	addMe.ToBinary(bits);
	lseek (fileDescriptor, PAGE_SIZE * (whichPage+1), SEEK_SET);
	write (fileDescriptor, bits, PAGE_SIZE);

	curLength = max(curLength, whichPage+1);

	delete [] bits;
}

off_t File :: GetLength () {
	return curLength;
}
