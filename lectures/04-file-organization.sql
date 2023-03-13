This version is more efficient.
int i1, i2;
if (i1 == i2) {}

string s1, s2;
if (s1 == s2) {}

How to store data in a database file?
- use binary representation instead of text

Heap file
- positional access at byte level --> seek function
- organize records into pages for faster access
- a list of pages, where a page contains an array of records
- provides sequential access to the pages of a file/table
- supports only appending pages at the end


- lineitem is 100 pages
- memory capacity is 10 pages

1-10
(1 out, 11 in)
(2 out, 12 in)
...
...

- page size is 128kb (131072 bytes)
 --> tradeoffs based on the size

- there is a separate file for every table
- in SQLite there is a single database file

- region
 - a single page is sufficient to store all the data
 - 5 records, average size is ~60 bytes --> ~300 bytes

 - 1|AMERICA|hs use ironic, even requests. s --> 44 bytes
 1 - int : 4 byte
 AMERICA - string : 7 bytes + 1 byte (end-of-string delimiter) --> 8 bytes
hs use ironic, even requests. s - string : 31 bytes + 1 byte --> 32 bytes

Project 3
1. Load data into database
 - create Files and copy content from *.tbl files to our binary File


File structure
 - page 0
   - int : number of pages in the file (4 bytes)
 - page 1
 - ...
 - page k


Page structure
 - int : number of records [n] (4 bytes)
 - record 1
   0|ALGERIA|0| haggle. carefully final deposits detect slyly agai|
  --  header
   - [0]  int (4 bytes) : length of record 1 [rl_1] --> [88]
   - [4]  int (4 bytes) : starting position (address, byte) of attribute 1 (n_nationkey) --> [20]
   - [8]  int (4 bytes) : starting position (address, byte) of attribute 2 (n_name) --> [24]
   - [12] int (4 bytes) : starting position (address, byte) of attribute 3 (n_regionkey) --> [32]
   - [16] int (4 bytes) : starting position (address, byte) of attribute 4 (n_comment) --> [36]
  --  data
   - [20] int (4 bytes) : value of n_nationkey --> 0
   - [24] string (8 bytes) : value of n_name --> 'ALGERIA\0'
   - [32] int (4 bytes) : value of n_regionkey --> 0
   - [36] string (52 bytes) : value of n_comment --> ' haggle. carefully final deposits detect slyly agai\0'
 - record 2
   1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|
   - [0]  int (4 bytes) : length of record 1 [rl_2] --> [115]
   - [4]  int (4 bytes) : starting position (address, byte) of attribute 1 (n_nationkey) --> [20]
   - [8]  int (4 bytes) : starting position (address, byte) of attribute 2 (n_name) --> [24]
   - [12] int (4 bytes) : starting position (address, byte) of attribute 3 (n_regionkey) --> [34]
   - [16] int (4 bytes) : starting position (address, byte) of attribute 4 (n_comment) --> [38]
   - [20] int (4 bytes) : value of n_nationkey --> 1
   - [24] string (10 bytes) : value of n_name --> 'ARGENTINA\0'
   - [34] int (4 bytes) : value of n_regionkey --> 1
   - [38] string (77 bytes) : value of n_comment --> 'al foxes promise slyly according to the regular accounts. bold requests alon\0'
 - ...
 - record n
   - int : length of record n [rl_n] (4 bytes)


Heap file (DBFile)
- returns the next record from the file, from the first one to the last one
- only sequential access to records is available
- MoveFirst() --> resets the record pointer (postion) to the first record in the file
- GetNext(Record&) --> returns the record at the current pointer (position) and increments the pointer

This is how we access records from a DBFile
MoveFirst()
while (GetNext(record)) {

}


// sequential access
int a[2000000000];
for (int i = 0; i < 2000000000; i++) {
  a[i] = i;
}

// random access
for (int i = 0; i < 2000000000; i++) {
  j = random(2000000000);
  a[j] = i;
}

