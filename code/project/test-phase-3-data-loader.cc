#include <vector>
#include <string>
#include <iostream>
#include <cstdlib>
#include <cstdio>

#include "Schema.h"
#include "Catalog.h"
#include "DBFile.h"

using namespace std;


int main (int argc, char* argv[]) {
	if (argc != 4) {
		cout << "Usage: phase-3-data-loader.out [table] [heap_file] [text_file]" << endl;
		return -1;
	}

	SString table(argv[1]);
	string heapFile = argv[2];
	// source
	string textFile = argv[3];

	SString catalogFile("catalog.sqlite");
	Catalog catalog(catalogFile);
	cout << catalog << endl; cout.flush();

	//write the code to load tuples from the text file to the heap file
	// extract schema from catalog
	Schema schema;
	bool ret = catalog.GetSchema(table, schema);
	if (false == ret) {
		cerr << "Error: table " << table << " does not exist in the database" << endl;
	}
	// create DBFile object
	DBFile dbFile;
	if (1 == dbFile.Create((char*) heapFile.c_str(), Heap)) {
		cerr << "Couldn't create dbFile" << endl;
		abort();
	}
	dbFile.Load(schema, (char*) textFile.c_str());
	dbFile.Close();

	SString fPath(heapFile);
	catalog.SetDataFile(table, fPath);
	
	return 0;
}
