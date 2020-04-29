#include <gtest/gtest.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include "Defs.h"
#include "Optimizer.h"
#include "QueryPlan.h"
#include "ParseTree.h"

using namespace std;

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
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

TEST( QueryPlanTest, CreateTable)
{

	FILE *cat_fp = fopen("location", "r");
	fscanf(cat_fp, "%s", catalog_path);
    // FILE *cat_fp = fopen("location", "r");
    // fscanf(cat_fp, "%s", catalog_path);
     // Statistics *stats = new Statistics();
     // stats->initStatistics();
     char *cnf = "CREATE TABLE customer_sample_table (c_custkey INTEGER, c_name STRING, c_address STRING, c_nationkey INTEGER, c_phone STRING, c_acctbal DOUBLE, c_mktsegment STRING, c_comment STRING) AS HEAP;";

   	yy_scan_string(cnf);
   	yyparse();

    // Optimizer optimizer(finalFunction, tables, boolean, groupingAtts, attsToSelect, distinctAtts, distinctFunc, stats);
		//
    // QueryPlan *queryplan = optimizer.OptimizedQueryPlan();
		QueryPlan *queryplan = new QueryPlan();
		int val = queryplan->ExecuteCreateTable(createTable);

		cout<<"\nCreated table "<<createTable->tableName<<endl;

    // int value = GetCount(root);
    // queryplan->PrintInOrder();
    // cout<<"\nExpected number of nodes in parse tree : 4";
    // cout<<"\nActual number of nodes in parse tree : "<<value<<endl;
		char* fname = "/home/desikan/Code/C++/a5/customer_sample_table.bin";

    ASSERT_FALSE(access(fname, F_OK) == -1 );
}

TEST( QueryPlanTest, DropTable)
{


    char *cnf = "DROP TABLE customer_sample_table;";

  	yy_scan_string(cnf);
  	yyparse();

		QueryPlan *queryplan = new QueryPlan();
		int val = queryplan->ExecuteDropTable(dropTableName);

			cout<<"\nDropped table "<<dropTableName<<endl;

			char* fname = "/home/desikan/Code/C++/a5/customer_sample_table.bin";

	    ASSERT_TRUE(access(fname, F_OK) == -1 );
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
