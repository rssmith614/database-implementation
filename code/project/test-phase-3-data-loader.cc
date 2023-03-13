#include <vector>
#include <string>
#include <iostream>
#include <cstdlib>
#include <cstdio>

#include "Schema.h"
#include "Catalog.h"

using namespace std;


int main (int argc, char* argv[]) {
	if (argc != 4) {
		cout << "Usage: phase-3-data-loader.out [table] [heap_file] [text_file]" << endl;
		return -1;
	}

	// record name
	string table = argv[1];
	// destination
	string heapFile = argv[2];
	// source
	string textFile = argv[3];

	SString catalogFile("catalog.sqlite");
	Catalog catalog(catalogFile);
	cout << catalog << endl; cout.flush();

	//write the code to load tuples from the text file to the heap file
	// create record for table
	// create file object
	// while (ExtractNextRecord)
		// 

	return 0;
}
