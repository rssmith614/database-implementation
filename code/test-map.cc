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

	for (int i = 0; i < 20; i++) {
		KeyInt ki = 20-i;
		SwapInt di = i*10;
		mii.Insert(ki, di);
	}
	cout << "map = ";
	cout << mii << endl;

	for (int i = 0; i < 10; i++) {
		KeyInt ki = i;
		SwapInt di;

		int isIn = mii.Find(ki, di);
		if (isIn == 0)
			cout << ki << " --> {}" << endl;
		else {
			cout << ki << " --> { ";
			while (!mii.AtEnd() && mii.CurrentKey().IsEqual(ki)) {
				cout << mii.CurrentData() << " ";
				mii.Advance();
			}
			cout << "}" << endl;
		}
	}

	cout << "map = ";
	cout << mii << endl;

	for (int i = 0; i < 30; i++) {
		KeyInt ki = i;

		int isIn = mii.IsThere(ki);
		if (isIn == 0)
			cout << ki << " --> {}" << endl;
		else {
			cout << ki << " --> { ";
			while (!mii.AtEnd() && mii.CurrentKey().IsEqual(ki)) {
				cout << mii.CurrentData() << " ";
				mii.Advance();
			}
			cout << "}" << endl;
		}
	}

	for (int i = 0; i < 5; i++) {
		KeyInt ki = i, kj;
		SwapInt dj;
		mii.Remove(ki, kj, dj);
	}
	cout << "map = ";
	cout << mii << endl;

	for (int i = 5; i < 10; i++) {
		KeyInt ki = i, kj;
		SwapInt dj;
		mii.Remove(ki, kj, dj);

		while (!mii.AtEnd() && mii.CurrentKey().IsEqual(ki)) {
			mii.Remove(ki, kj, dj);
		}

	}
	cout << "map = ";
	cout << mii << endl;

	return 0;
}
