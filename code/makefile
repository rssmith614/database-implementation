CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif


all: test-phase-1.out test-phase-2.out test-phase-3-data-loader.out test-query.out


test-phase-1.out: Schema.o Catalog.o test-phase-1.o
	$(CC) -o test-phase-1.out test-phase-1.o Catalog.o Schema.o -lsqlite3
	
test-phase-1.o: test-phase-1.cc
	$(CC) -c test-phase-1.cc


test-phase-2.out: QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryOptimizer.o QueryCompiler.o test-phase-2.o
	$(CC) -o test-phase-2.out test-phase-2.o QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryOptimizer.o QueryCompiler.o -lfl -lsqlite3
	
test-phase-2.o:	test-phase-2.cc
	$(CC) -c test-phase-2.cc


test-phase-3-data-loader.out: QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryOptimizer.o QueryCompiler.o test-phase-3-data-loader.o
	$(CC) -o test-phase-3-data-loader.out test-phase-3-data-loader.o QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryOptimizer.o QueryCompiler.o -lfl -lsqlite3
	
test-phase-3-data-loader.o: test-phase-3-data-loader.cc
	$(CC) -c test-phase-3-data-loader.cc


test-query.out: QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryOptimizer.o QueryCompiler.o test-query.o
	$(CC) -o test-query.out test-query.o QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryOptimizer.o QueryCompiler.o -lfl -lsqlite3
	
test-query.o: test-query.cc
	$(CC) -c test-query.cc


test-list.out: test-list.o
	$(CC) -o test-list.out test-list.o
	
test-list.o: test-list.cc List.h List.cc Swapify.h Swapify.cc
	$(CC) -c test-list.cc


Catalog.o: Catalog.h Catalog.cc	
	$(CC) -c Catalog.cc
	
Schema.o: Config.h Schema.h Schema.cc	
	$(CC) -c Schema.cc

Record.o: Schema.cc Record.cc
	$(CC) -c Record.cc

File.o: Schema.cc Record.cc File.cc
	$(CC) -c File.cc

DBFile.o: Schema.cc Record.cc File.cc DBFile.cc
	$(CC) -c DBFile.cc

Comparison.o: Schema.cc Record.cc Comparison.cc
	$(CC) -c Comparison.cc

Function.o: Schema.cc Record.cc Function.cc
	$(CC) -c Function.cc

RelOp.o: Schema.cc Record.cc Comparison.cc RelOp.cc	
	$(CC) -c RelOp.cc
	
QueryOptimizer.o: Schema.cc Record.cc Comparison.cc RelOp.cc QueryOptimizer.cc	
	$(CC) -c QueryOptimizer.cc

QueryCompiler.o: Schema.cc Record.cc Comparison.cc RelOp.cc QueryOptimizer.cc QueryCompiler.cc	
	$(CC) -c QueryCompiler.cc


QueryParser.o: QueryParser.y
	yacc --defines=QueryParser.h -o QueryParser.c QueryParser.y
	sed $(tag) QueryParser.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	$(CC) -c QueryParser.c

QueryLexer.o: QueryLexer.l
	lex -o QueryLexer.c QueryLexer.l
	gcc -c QueryLexer.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f QueryLexer.c
	rm -f QueryParser.c
	rm -f QueryParser.h
