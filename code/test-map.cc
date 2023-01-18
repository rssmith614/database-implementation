#include <iostream>
#include <cstdlib>
#include <cstdio>

#include "Map.cc"

using namespace std;


int main (int argc, char* argv[]) {
	MapIntInt mii;

	for (int i = 0; i < 20; i++) {
		KeyInt ki = i;
		SwapInt di = i*100;
		mii.Insert(ki, di);
	}
	cout << "map = ";
	cout << mii << endl;

	return 0;
}
