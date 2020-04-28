CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

mygtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Heap.o Sorted.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryPlan.o Optimizer.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o mygtest.o -lpthread
	$(CC) -o mygtest.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Heap.o Sorted.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o QueryPlan.o Optimizer.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o mygtest.o -lpthread -lgtest

a5.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Heap.o Sorted.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o QueryPlan.o Optimizer.o y.tab.o lex.yy.o main.o
	$(CC) -o a5.out Record.o Comparison.o ComparisonEngine.o Schema.o Function.o File.o Heap.o Sorted.o DBFile.o Pipe.o BigQ.o RelOp.o Statistics.o QueryPlan.o Optimizer.o y.tab.o lex.yy.o main.o -lpthread

main.o: main.cc
	$(CC) -g -c main.cc

a4-1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Heap.o Sorted.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o  -lpthread
	$(CC) -o a4-1.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Heap.o Sorted.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o y.tab.o lex.yy.o test.o  -lpthread

a2-2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Heap.o Sorted.o Pipe.o y.tab.o lex.yy.o a2-2test.o -lpthread
	$(CC) -o a2-2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Heap.o Sorted.o Pipe.o y.tab.o lex.yy.o a2-2test.o -lpthread

a2test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Heap.o Sorted.o Pipe.o y.tab.o lex.yy.o a2-test.o
	$(CC) -o a2test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Heap.o Sorted.o Pipe.o y.tab.o lex.yy.o a2-test.o -lpthread

a2-2test.o: a2-2test.cc
	$(CC) -g -c a2-2test.cc

a2-test.o: a2-test.cc
	$(CC) -g -c a2-test.cc

mygtest.o: mygtest.cc
	$(CC) -g -c mygtest.cc

test.o: test.cc
	$(CC) -g -c test.cc

Optimizer.o: Optimizer.cc
	$(CC) -g -c Optimizer.cc

QueryPlan.o:QueryPlan.cc
	$(CC) -g -c QueryPlan.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Heap.o: Heap.cc
	$(CC) -g -c Heap.cc

Sorted.o: Sorted.cc
	$(CC) -g -c Sorted.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c y.tab.c

yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/"
	g++ -c yyfunc.tab.c

lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c


clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
