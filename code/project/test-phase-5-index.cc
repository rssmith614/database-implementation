#include <iostream>

#include "Catalog.h"
#include "DBFile.h"
#include "BTreeIndex.h"

using namespace std;

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cout << "Usage: test-phase-5-index.out [index_name] [table_name] [attribute_name]" << endl;
		return -1;
    }

    string indexName = argv[1];
	SString tableName(argv[2]);
	string attName = argv[3];

    SString catalogFile("catalog.sqlite");
    Catalog catalog(catalogFile);
    Schema schema;

    if (!catalog.GetSchema(tableName, schema))
        exit(1);

    BTreeIndex idx;
    cout << "building index " << indexName << " on attribute " << attName << endl;
    idx.Build(indexName, tableName, attName, schema);

    // SInt lookFor(60000);
    // off_t onPage;
    // int ret = idx.Find(lookFor, onPage);
    // if (ret == 0) {
    //     cout << "found " << lookFor << " on page " << onPage << endl;
    // } else {
    //     cout << "couldn't find " << lookFor << endl;
    // }

    // SInt lower(34691);
    // SInt upper(46084);
    // off_t start, end;
    // ret = idx.FindRange(lower, upper, start, end);
    // if (ret == 0) {
    //     cout << "values between " << lower << " to " << upper << " can be found on pages " << start << "-" << end << endl;
    // } else {
    //     cout << "key error: " << lower << " or " << upper << endl;
    // }
}