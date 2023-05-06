#include "BTreeIndex.h"

BTreeIndex::BTreeIndex(): idxFileName(""), currentIdxPagePos(0) {}

BTreeIndex::~BTreeIndex() {}

BTreeIndex::BTreeIndex(const BTreeIndex& _copyMe):
    idxFile(_copyMe.idxFile), idxFileName(_copyMe.idxFileName) {
        // m.CopyFrom(_copyMe.m);
}

void BTreeIndex::Read(string fileName) {
    idxFileName = fileName;
    idxFile.Open(-1, (char*) idxFileName.c_str());
}

void BTreeIndex::Print(ostream &_os, off_t pageToPrint) {
    idxFile.GetPage(currentIdxPage, pageToPrint);
    currentIdxPage.Print(_os);

    vector<off_t> ptrs;
    if (0 == currentIdxPage.GetChildren(ptrs)) {
        for (auto ptr : ptrs) {
            Print(_os, ptr);
        }
    }
}

int BTreeIndex::Build(string indexName, string tblName, SString attName, Schema& schema) {
    string dbFileName = "../data/" + tblName + ".dat";
	dbFile.Open((char*) dbFileName.c_str());

    idxFileName = "../data/" + indexName + ".idx";
    idxFile.Open(0, (char*) idxFileName.c_str());

    attCol = schema.Index(attName);

    Record current;
    int recCount = 0;

    while (0 == dbFile.GetNext(current)) {
        // always start from root
        if (firstPageWritten) {
            currentIdxPagePos = 0;
            idxFile.GetPage(currentIdxPage, currentIdxPagePos);
        }
        int key = ((int*) current.GetColumn(attCol))[0];
        SInt skey(key);
        off_t ptr = dbFile.GetCurrentPage();

        // if (key == 60000)
        //     cout << 60000 << " was read from dbfile page " << ptr << endl;

        // if (key == 34691)
        //     cout << 34691 << " was read from dbfile page " << ptr << endl;

        // if (key == 46084)
        //     cout << 46084 << " was read from dbfile page " << ptr << endl;

        off_t ret = currentIdxPage.Add(key, ptr);

        while (ret != 0) {
            if (-1 == ret) { // insert failed because page was full
                vector<int> keys;
                vector<off_t> ptrs;
                // split the full node
                currentIdxPage.Split(keys, ptrs);
                off_t left, right;
                
                if (currentIdxPagePos == 0) { // if the node we just split was the root
                    // put first half on a "new" page (assign new page number)
                    left = ++maxPageId;
                    
                    // define its parent as the root
                    currentIdxPage.SetParent(0);
                    
                    right = ++maxPageId;
                    // make left child point to its sibling
                    currentIdxPage.SetSibling(right);
                    currentIdxPage.SetPageNumber(left, ptrs);
                    // save left page
                    idxFile.AddPage(currentIdxPage, left);
                    // start with empty page for right
                    currentIdxPage.EmptyItOut();
                    // put second half in new page with unique page number
                    currentIdxPage.Generate(keys, ptrs);
                    currentIdxPage.SetPageNumber(right, ptrs);
                    // parent is root
                    currentIdxPage.SetParent(0);
                    // save
                    idxFile.AddPage(currentIdxPage, right);
                    // create the new root that points to children
                    PrepareNewRoot(keys[0], left, right);

                } else { // we split a leaf node that was not the root
                    left = currentIdxPagePos;
                    right = ++maxPageId;
                    off_t currentParent = currentIdxPage.GetParent();
                    // make left child point to its sibling
                    currentIdxPage.SetSibling(right);
                    // save
                    idxFile.AddPage(currentIdxPage, left);

                    // start with empty page
                    currentIdxPage.EmptyItOut();
                    // put second half in new page
                    currentIdxPage.Generate(keys, ptrs);
                    currentIdxPage.SetPageNumber(right, ptrs);
                    currentIdxPage.SetParent(currentParent);
                    idxFile.AddPage(currentIdxPage, right);
                    // promote the smallest key of right node
                    InsertIntermediate(currentIdxPage.GetParent(), keys[0], right);
                }

                ret = 0;

            } else if (ret > 0) { // insert failed because we are on intermediate page
                // move to next page to search
                currentIdxPagePos = ret;
                idxFile.GetPage(currentIdxPage, currentIdxPagePos);

                // try to insert again
                ret = currentIdxPage.Add(key, ptr);

                if (ret == 0) {
                    idxFile.AddPage(currentIdxPage, currentIdxPagePos);
                    
                }
            }
        }

    }

    if (!firstPageWritten) {
        idxFile.AddPage(currentIdxPage, 0);
    }
    
    cout << "index built with " << maxPageId+1 << " pages" << endl;
    idxFile.Close();
    // Print(cout, 0);
	return 0;
}

void BTreeIndex :: PrepareNewRoot (int key, off_t left, off_t right) {
    currentIdxPage.EmptyItOut();
    vector<int> keys;
    keys.push_back(key);
    vector<off_t> ptrs;
    ptrs.push_back(left);   ptrs.push_back(right);

    currentIdxPage.Generate(keys, ptrs);
    vector<off_t> ptrsThatNeedNewParents;
    currentIdxPage.SetPageType(IndexPage::INTERMEDIATE);
    currentIdxPage.SetPageNumber(0, ptrsThatNeedNewParents);
    currentIdxPage.SetParent(-1);
    idxFile.AddPage(currentIdxPage, 0);
    for (auto ptr : ptrsThatNeedNewParents) {
        idxFile.GetPage(currentIdxPage, ptr);
        currentIdxPage.SetParent(0);
    }

    firstPageWritten = true;
}

void BTreeIndex :: InsertIntermediate(off_t where, int key, off_t ptr) {
    idxFile.GetPage(currentIdxPage, where);
    off_t currentParent = currentIdxPage.GetParent();
    int ret = currentIdxPage.AddIntermediate(key, ptr);

    if (-1 == ret) {
        // intermediate page was full :(
        vector<int> keys;
        vector<off_t> ptrs;
        vector<off_t> ptrsThatNeedNewParents;
        currentIdxPage.Split(keys, ptrs);
        off_t left, right;
        if (where == 0) {
            int newRootKey = currentIdxPage.PromoteEnd();
            // put first half on a "new" page (assign new page number)
            left = ++maxPageId;
            currentIdxPage.SetPageNumber(left, ptrsThatNeedNewParents);
            currentIdxPage.SetPageType(IndexPage::INTERMEDIATE);
            // define its parent as the root
            currentIdxPage.SetParent(0);
            
            // save left page
            idxFile.AddPage(currentIdxPage, left);
            for (auto ptr : ptrsThatNeedNewParents) {
                idxFile.GetPage(currentIdxPage, ptr);
                currentIdxPage.SetParent(left);
                idxFile.AddPage(currentIdxPage, ptr);
            }
            right = ++maxPageId;
            // open right page (empty right now)
            currentIdxPage.EmptyItOut();
            currentIdxPage.SetPageType(IndexPage::INTERMEDIATE);
            // put second half in new page with unique page number
            currentIdxPage.Generate(keys, ptrs);
            currentIdxPage.SetPageNumber(right, ptrsThatNeedNewParents);
            // parent is root
            currentIdxPage.SetParent(0);
            // save
            idxFile.AddPage(currentIdxPage, right);
            for (auto ptr : ptrsThatNeedNewParents) {
                idxFile.GetPage(currentIdxPage, ptr);
                currentIdxPage.SetParent(right);
                idxFile.AddPage(currentIdxPage, ptr);
            }
            // create the new root that points to children
            PrepareNewRoot(newRootKey, left, right);
        } else {
            int newKey = currentIdxPage.PromoteEnd();
            // save
            left = where;
            idxFile.AddPage(currentIdxPage, left);

            right = ++maxPageId;
            // start with empty page
            currentIdxPage.EmptyItOut();
            
            currentIdxPage.Generate(keys, ptrs);
            currentIdxPage.SetPageNumber(right, ptrsThatNeedNewParents);
            currentIdxPage.SetPageType(IndexPage::INTERMEDIATE);
            // put second half in new page
            // parent is the same as the the node we just split off of
            
            idxFile.AddPage(currentIdxPage, right);

            for (auto ptr : ptrsThatNeedNewParents) {
                idxFile.GetPage(currentIdxPage, ptr);
                currentIdxPage.SetParent(right);
                idxFile.AddPage(currentIdxPage, ptr);
            }
            // promote the largest key of left node
            InsertIntermediate(currentParent, newKey, right);
        }
    } else {
        idxFile.AddPage(currentIdxPage, where);
    }
}

int BTreeIndex::Find(SInt key, off_t &pageNumber) {
    idxFile.GetPage(currentIdxPage, 0);
    int ret = currentIdxPage.Find(key, pageNumber);
    while (1 == ret) {
        idxFile.GetPage(currentIdxPage, pageNumber);
        ret = currentIdxPage.Find(key, pageNumber);
    }

    return ret;
}

int BTreeIndex::FindRange(SInt lowerBound, SInt upperBound, unordered_set<off_t> &pages) {
    // find lower bound first
    off_t tempPage;
    int ret = -1;
    if (lowerBound > 0) {
        ret = Find(lowerBound, tempPage);
    } else {
        ret = Find(upperBound, tempPage);
    }
    if (-1 == ret) {
        cout << "Error: key " << ((lowerBound > 0) ? lowerBound : upperBound) << " not found in " << idxFileName << endl;
        cout << "To use index, key must exist in table" << endl;
        return ret;
    } else {
        // save all ptrs (except for sibling) that occur on and after key
        unordered_set<off_t> temp;
        ret = currentIdxPage.FindRange(lowerBound, upperBound, temp, tempPage);
        pages.insert(temp.begin(), temp.end());
        while (ret == 1) {
            ret = currentIdxPage.FindRange(lowerBound, upperBound, temp, tempPage);
            pages.insert(temp.begin(), temp.end());
        }
        return ret;
    }
}