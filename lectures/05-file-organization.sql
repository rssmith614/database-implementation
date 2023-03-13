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
   - [0]  int (4 bytes) : length of record 1 [rl_1] --> [88]
   - [4]  int (4 bytes) : starting position (address, byte) of attribute 1 (n_nationkey) --> [20]
   - [8]  int (4 bytes) : starting position (address, byte) of attribute 2 (n_name) --> [24]
   - [12] int (4 bytes) : starting position (address, byte) of attribute 3 (n_regionkey) --> [32]
   - [16] int (4 bytes) : starting position (address, byte) of attribute 4 (n_comment) --> [36]
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


orders
insert into attributes values('orders', 0, 'o_orderkey', 'INTEGER', 15000);
insert into attributes values('orders', 1, 'o_custkey', 'INTEGER', 1000);
insert into attributes values('orders', 2, 'o_orderstatus', 'STRING', 3);
insert into attributes values('orders', 3, 'o_totalprice', 'FLOAT', 14996);
insert into attributes values('orders', 4, 'o_orderdate', 'STRING', 2401);
insert into attributes values('orders', 5, 'o_orderpriority', 'STRING', 5);
insert into attributes values('orders', 6, 'o_clerk', 'STRING', 1000);
insert into attributes values('orders', 7, 'o_shippriority', 'STRING', 1);
insert into attributes values('orders', 8, 'o_comment', 'STRING', 15000);

39|818|O|326565.37|1996-09-20|3-MEDIUM|Clerk#000000659|0|ole express, ironic requests: ir|
  - [0]  int (4 bytes) : length of record 1 [rl_2] --> [129]
  - [4]  int (4 bytes) : starting position (address, byte) of attribute 1 (o_orderkey) --> [40]
  - [8]  int (4 bytes) : starting position (address, byte) of attribute 2 (o_custkey) --> [44]
  - [12] int (4 bytes) : starting position (address, byte) of attribute 3 (o_orderstatus) --> [48]
  - [16] int (4 bytes) : starting position (address, byte) of attribute 4 (o_totalprice) --> [50]
  - [20] int (4 bytes) : starting position (address, byte) of attribute 4 (o_orderdate) --> [58]
  - [24] int (4 bytes) : starting position (address, byte) of attribute 4 (o_orderpriority) --> [69]
  - [28] int (4 bytes) : starting position (address, byte) of attribute 4 (o_clerk) --> [78]
  - [32] int (4 bytes) : starting position (address, byte) of attribute 4 (o_shippriority) --> [94]
  - [36] int (4 bytes) : starting position (address, byte) of attribute 4 (o_comment) --> [96]
  - [40] int (4 bytes) : value of o_orderkey --> 39
  - [44] string (10 bytes) : value of o_custkey --> 818
  - [48] int (4 bytes) : value of o_orderstatus --> 'O\0'
  - [50] string (77 bytes) : value of o_totalprice --> 326565.37
  - [58] string (77 bytes) : value of o_orderdate --> '1996-09-20\0'
  - [69] string (77 bytes) : value of o_orderpriority --> '3-MEDIUM\0'
  - [78] string (77 bytes) : value of o_clerk --> 'Clerk#000000659\0'
  - [94] string (77 bytes) : value of o_shippriority --> '0\0'
  - [96] string (77 bytes) : value of o_comment --> 'ole express, ironic requests: ir\0'

- column-store databases
