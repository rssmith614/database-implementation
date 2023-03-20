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

	string table = argv[1];
	string heapFile = argv[2];
	string textFile = argv[3];

	SString catalogFile("catalog.sqlite");
	Catalog catalog(catalogFile);
	cout << catalog << endl; cout.flush();

	Schema tblSchema; catalog.GetSchema(table.c_str(), tblSchema);
	DBFile file;
	file.Create((heapFile.c_str()), Heap);

	file.Open(heapFile.c_str());

	file.Load(tblSchema, textFile);

	file.Close();

	return 0;
}
