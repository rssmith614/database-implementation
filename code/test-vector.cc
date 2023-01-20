#include <iostream>
#include <cstdlib>
#include <cstdio>

#include "Vector.cc"

using namespace std;


int main (int argc, char* argv[]) {
	IntVector iv;

	for (int i = 0; i < 120; i++) {
		SwapInt si = i;
		iv.Append(si);
	}
	cout << "vector = ";
	cout << iv << endl;

	for (int i = 0; i < 100; i++) {
		SwapInt si;
		iv.Remove(0, si);
	}
	cout << "vector = ";
	cout << iv << endl;

	for (int i = 0; i < 100; i++) {
		SwapInt si = i;
		iv.Insert(100-i, si);
	}
	cout << "vector = ";
	cout << iv << endl;

	for (int i = 0; i <= 100; i++) {
		cout << "v[" << 100-i << "] = " << iv[100-i] << endl;
	}

	return 0;
}
