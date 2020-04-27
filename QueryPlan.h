
#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include "RelOp.h"
#include "Pipe.h"
#include <map>
#include <string.h>
#include "ParseTree.h"
#include "DBFile.h"
#include <iostream>
#include <fstream>
#include <vector>

using namespace std;


class QueryPlanNode {
public:


	QueryNodeType opType;

	QueryPlanNode *parent;
	QueryPlanNode *left;
	QueryPlanNode *right;

	int lPipeId;
	int rPipeId;
	int outPipeId;
	FILE *outFile;
	string dbfilePath;

	CNF *cnf;
	Record *literal;
	Schema *outputSchema;
	Function *function;

	OrderMaker *orderMaker;
	int *keepMe;
	int numAttsInput, numAttsOutput;

	QueryPlanNode();
	~QueryPlanNode();
};

class QueryPlan {
public:
	QueryPlan();
	virtual ~QueryPlan();

	QueryPlanNode *root;

	int pipeNum;
	map<int, Pipe*> pipes;
	vector<RelationalOp *> operators;

	int dbNum;
	DBFile *dbs[10];

	char* output;

	void PrintNode(QueryPlanNode *);
	void PrintInOrder();

	void ExecuteNode(QueryPlanNode *);
	int ExecuteQueryPlan();
	int ExecuteQuery();
	int ExecuteCreateTable(CreateTable*);
	int ExecuteInsertFile(InsertFile*);
	int ExecuteDropTable(char *);


};

#endif
