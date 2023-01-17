#ifndef _LIST_H
#define _LIST_H

// Basic doubly linked list
// Type requires Swap

#include "Swapify.h"

using namespace std;


template <class Type>
class List {
public:
	// basic constructor function
	List ();

	// destructor function
	virtual ~List ();

	// swap operator
	void Swap (List&);

	// get the content from another list (without destroying it)
	void CopyFrom(List&);

	// add to current pointer position
	void Insert (Type&);

	// add at the end of the list
	void Append (Type&);

	// look at the current item
	Type& Current ();

	// remove from current position
	void Remove (Type&);

	// move the current pointer position backward through the list
	void Retreat ();

	// move the current pointer position forward through the list
	void Advance ();

	// operations to check the size of both sides
	int LeftLength ();
	int RightLength ();
	int Length();

	// operations to consult state
	bool AtStart ();
	bool AtEnd ();

	// operations to swap the left and right sides of two lists
	void SwapLefts (List&);
	void SwapRights (List&);

	// operations to move the the start of end of a list
	void MoveToStart ();
	void MoveToFinish ();

protected:
	struct Node {
		// data
		Type data;
		Node* next;
		Node* previous;

		// constructor
		Node () : next (0), previous (0) {}
		virtual ~Node () {}
	};

	struct Header {
		// data
		Node* first;
		Node* last;
		Node* current;

		int leftSize;
		int rightSize;
	};

	// the list itself is pointed to by this pointer
	Header* list;

	// making the default constructor and = operator private so  the list
	// can only be propagated by Swap. Otherwise subtle bugs can appear
	List(List&);
	List operator= (List&);
};

typedef List<SwapDouble> DoubleList;
typedef List<SwapInt> IntList;
typedef List<SwapString> StringList;

#endif
