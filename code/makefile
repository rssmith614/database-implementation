CC = g++
CCFLAGS = -g -O0 -Wno-deprecated -Wno-c++11-extensions -mmacosx-version-min=13.3

tag = -i

OBJDIR = object
HEADERDIR = headers
EXECDIR = execs
SRCDIR = source

UTILS = $(addprefix $(OBJDIR)/,Map.o Vector.o List.o Swapify.o)

COMPILER = $(addprefix $(OBJDIR)/,QueryParser.o QueryLexer.o)

PHASE_ONE = $(addprefix $(OBJDIR)/,Schema.o Catalog.o test-phase-1.o)
PHASE_TWO = $(addprefix $(OBJDIR)/,Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o test-phase-2.o)
PHASE_THREE_DATA_LOADER = $(addprefix $(OBJDIR)/,Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o BTreeIndex.o test-phase-3-data-loader.o)
PHASE_THREE = $(addprefix $(OBJDIR)/,Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o test-phase-3.o)
QUERY = $(addprefix $(OBJDIR)/,Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o BTreeIndex.o test-query.o)
PHASE_FIVE_INDEX = $(addprefix $(OBJDIR)/,Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o QueryCompiler.o BTreeIndex.o test-phase-5-index.o)

OBJ = $(wildcard $(OBJDIR)/*.o)

ifdef linux
	tag = -n
endif

init:
	mkdir -p $(OBJDIR)
	mkdir -p $(EXECDIR)

$(OBJDIR)/%.o: $(HEADERDIR)/%.cc
	$(CC) $(CCFLAGS) -o $@ -c $^ -I $(HEADERDIR)

$(OBJDIR)/%.o: $(SRCDIR)/%.cc
	$(CC) $(CCFLAGS) -o $@ -c $^ -I $(HEADERDIR)

$(OBJDIR)/%.o: project/%.cc
	$(CC) $(CCFLAGS) -o $@ -c $^ -I $(HEADERDIR)

$(OBJDIR)/QueryParser.o: compiler/QueryParser.y
	yacc --defines=headers/QueryParser.h -o source/QueryParser.c compiler/QueryParser.y
	sed $(tag) "" "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" source/QueryParser.c
	$(CC) $(CCFLAGS) -o object/QueryParser.o -c source/QueryParser.c -I headers/

$(OBJDIR)/QueryLexer.o: compiler/QueryLexer.l
	lex -o source/QueryLexer.c compiler/QueryLexer.l
	gcc -o object/QueryLexer.o -c source/QueryLexer.c -I headers/

utils: $(UTILS)

test-phase-1.out: $(PHASE_ONE)
	$(CC) $(CCFLAGS) -o $(EXECDIR)/$@ $^ -lsqlite3

test-phase-2.out: $(COMPILER) $(PHASE_TWO)
	$(CC) $(CCFLAGS) -o $(EXECDIR)/$@ $^ -ll -lsqlite3

test-phase-3-data-loader.out: $(COMPILER) $(PHASE_THREE_DATA_LOADER)
	$(CC) $(CCFLAGS) -o $(EXECDIR)/$@ $^ -ll -lsqlite3

test-phase-3.out: $(COMPILER) $(PHASE_THREE)
	$(CC) $(CCFLAGS) -o $(EXECDIR)/$@ $^ -ll -lsqlite3

test-query.out: $(COMPILER) $(QUERY)
	$(CC) $(CCFLAGS) -o $(EXECDIR)/$@ $^ -ll -lsqlite3

test-phase-5-index.out: $(PHASE_FIVE_INDEX)
	$(CC) $(CCFLAGS) -o $(EXECDIR)/$@ $^ -ll -lsqlite3

project: test-phase-1.out test-phase-2.out test-phase-3-data-loader.out test-phase-3.out test-query.out

all: init utils project


clean: 
	rm -f object/*.o
	rm -f execs/*.out
	rm -f source/QueryLexer.c
	rm -f source/QueryParser.c
	rm -f headers/QueryParser.h
	rm -f output.txt sqlite_output.txt output_diff.txt