#include <iostream>
#include <fstream>
#include "Defs.h"
#include "Optimizer.h"
#include "QueryPlan.h"
#include "ParseTree.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
}

extern struct CreateTable *createTable;
extern struct InsertFile *insertFile;
extern char *dropTableName;
extern char *setOutPut;

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

extern int quit;

char catalog_path[50];
char dbfile_dir[50];
char tpch_dir[50];
int RUNLEN;

int main(void) {

	FILE *cat_fp = fopen("location", "r");
	fscanf(cat_fp, "%s", catalog_path);
	fscanf(cat_fp, "%s", dbfile_dir);
	fscanf(cat_fp, "%s", tpch_dir);
	fscanf(cat_fp, "%d", &RUNLEN);
	fclose(cat_fp);



	//cout <<"sql->";
	yyparse();
	QueryPlan *queryPlan = new QueryPlan();
	if(queryPlan->ExecuteInsertFile(insertFile)) {
		cout <<"Loaded file "<<insertFile->fileName<<" into " <<insertFile->tableName<<endl;
	}

	// Statistics *stats = new Statistics();
	// stats->initStatistics();
	//
	// Optimizer optimizer(finalFunction, tables, boolean, groupingAtts, attsToSelect, distinctAtts, distinctFunc, stats);
	//
	// QueryPlan *queryplan = optimizer.OptimizedQueryPlan();
	// if(queryplan == NULL)
	// {
	// 	cerr <<"Unable to build query plan"<<endl;
	// 	exit(0);
	// }
	// queryplan->ExecuteQuery();
	//queryplan->PrintInOrder();
}
