#include <iostream>
#include <string>

#include "Swapify.cc"
#include "SQLiteODBC.h"

using namespace std;


int main (int argc, char* argv[]) {
	if (argc != 2) {
		cout << "Usage: main [sqlite_file]" << endl;
		return -1;
	}

	SString dbFile(argv[1]);
	SQLiteODBC sj(dbFile);
	
	sj.OpenConnection();

	sj.DropTables();
	sj.CreateTables();
	sj.PopulateTables();

	SString maker("A"), product("Laptop");

	sj.PCsByMaker(maker);
	sj.ProductByMaker(product, maker);
	sj.AllProductsByMaker(maker);

	sj.CloseConnection();

	return 0;
}
