CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif


init:
	mkdir -p object
	mkdir -p execs


test-vector.out: test-vector.o
	$(CC) -o execs/test-vector.out object/test-vector.o
	
test-vector.o: tests/test-vector.cc headers/Vector.h headers/Vector.cc headers/Swapify.h headers/Swapify.cc
	$(CC) -o object/test-vector.o -c tests/test-vector.cc -I headers/

test-list.out: test-list.o
	$(CC) -o execs/test-list.out object/test-list.o
	
test-list.o: tests/test-list.cc headers/List.h headers/List.cc headers/Swapify.h headers/Swapify.cc
	$(CC) -o object/test-list.o -c tests/test-list.cc -I headers/

test-map.out: test-map.o
	$(CC) -o execs/test-map.out object/test-map.o
	
test-map.o: tests/test-map.cc headers/Map.h headers/Map.cc headers/Swapify.h headers/Swapify.cc headers/Keyify.h headers/Keyify.cc
	$(CC) -o object/test-map.o -c tests/test-map.cc -I headers/

test-sqlite.out: test-sqlite.o
	$(CC) -o execs/test-sqlite.out object/test-sqlite.o -lsqlite3

test-sqlite.o: tests/test-sqlite.cc headers/Swapify.h headers/Swapify.cc
	$(CC) -o object/test-sqlite.o -c tests/test-sqlite.cc -I headers/

tests: test-vector.out test-list.out test-map.out test-sqlite.out


Schema.o: source/Schema.cc
	$(CC) -o object/Schema.o -c source/Schema.cc -I headers

Catalog.o: source/Schema.cc source/Catalog.cc
	$(CC) -o object/Catalog.o -c source/Catalog.cc -I headers
	
Record.o: source/Schema.cc source/Record.cc
	$(CC) -o object/Record.o -c source/Record.cc -I headers

File.o: source/Schema.cc source/Record.cc source/File.cc
	$(CC) -o object/File.o -c source/File.cc -I headers

DBFile.o: source/Schema.cc source/Record.cc source/File.cc source/DBFile.cc
	$(CC) -o object/DBFile.o -c source/DBFile.cc -I headers

Comparison.o: source/Schema.cc source/Record.cc source/Comparison.cc
	$(CC) -o object/Comparison.o -c source/Comparison.cc -I headers

Function.o: source/Schema.cc source/Record.cc source/Function.cc
	$(CC) -o object/Function.o -c source/Function.cc -I headers

RelOp.o: source/Schema.cc source/Record.cc source/Comparison.cc source/RelOp.cc	
	$(CC) -o object/RelOp.o -c source/RelOp.cc -I headers
	
QueryCompiler.o: source/Schema.cc source/Record.cc source/Comparison.cc source/RelOp.cc source/QueryCompiler.cc
	$(CC) -o object/QueryCompiler.o -c source/QueryCompiler.cc -I headers


QueryParser.o: compiler/QueryParser.y
	yacc --defines=headers/QueryParser.h -o source/QueryParser.c compiler/QueryParser.y
	sed $(tag) source/QueryParser.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	$(CC) -o object/QueryParser.o -c source/QueryParser.c -I headers/

QueryLexer.o: compiler/QueryLexer.l
	lex -o source/QueryLexer.c compiler/QueryLexer.l
	gcc -o object/QueryLexer.o -c source/QueryLexer.c -I headers/

compiler: QueryParser.o QueryLexer.o


test-phase-1.out: Schema.o Catalog.o test-phase-1.o
	$(CC) -o execs/test-phase-1.out object/test-phase-1.o object/Catalog.o object/Schema.o -lsqlite3
	
test-phase-1.o: project/test-phase-1.cc
	$(CC) -o object/test-phase-1.o -c project/test-phase-1.cc -I headers/

test-phase-2.out: compiler Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o test-phase-2.o
	$(CC) -o execs/test-phase-2.out object/test-phase-2.o object/QueryParser.o object/QueryLexer.o object/Schema.o object/Record.o object/File.o object/DBFile.o object/Comparison.o object/Function.o object/RelOp.o object/Catalog.o object/QueryCompiler.o -lfl -lsqlite3
	
test-phase-2.o:	project/test-phase-2.cc
	$(CC) -o object/test-phase-2.o -c project/test-phase-2.cc -I headers/

test-phase-3-data-loader.out: compiler Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o test-phase-3-data-loader.o
	$(CC) -o execs/test-phase-3-data-loader.out object/test-phase-3-data-loader.o object/QueryParser.o object/QueryLexer.o object/Schema.o object/Record.o object/File.o object/DBFile.o object/Comparison.o object/Function.o object/RelOp.o object/Catalog.o object/QueryCompiler.o -lfl -lsqlite3
	
test-phase-3-data-loader.o: project/test-phase-3-data-loader.cc
	$(CC) -o object/test-phase-3-data-loader.o -c project/test-phase-3-data-loader.cc -I headers/

test-query.out: compiler Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o test-query.o
	$(CC) -o execs/test-query.out object/test-query.o object/QueryParser.o object/QueryLexer.o object/Schema.o object/Record.o object/File.o object/DBFile.o object/Comparison.o object/Function.o object/RelOp.o object/Catalog.o object/QueryCompiler.o -lfl -lsqlite3
	
test-query.o: project/test-query.cc
	$(CC) -o object/test-query.o -c project/test-query.cc -I headers/

project: test-phase-1.out test-phase-2.out test-phase-3-data-loader.out test-query.out


all: init tests project


clean: 
	rm -f object/*.o
	rm -f tests/*.out
	rm -f execs/*.out
	rm -f source/QueryLexer.c
	rm -f source/QueryParser.c
	rm -f headers/QueryParser.h
