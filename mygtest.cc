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

int GetCount(QueryPlanNode *node) {
	  if (node->left)
		    return 1+GetCount(node->left);
		else if(node->right)
		    return 1+GetCount(node->right);
    else
        return 1;
}


TEST( QueryPlanTest, QueryPlanGeneration)
{
    FILE *cat_fp = fopen("location", "r");
    fscanf(cat_fp, "%s", catalog_path);
    Statistics *stats = new Statistics();
    stats->initStatistics();

    char *cnf = "SELECT SUM DISTINCT (n.n_nationkey + r.r_regionkey) FROM nation AS n, region AS r, customer AS c WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey = c.c_nationkey) AND (n.n_nationkey > 10)GROUP BY r.r_regionkey";

  	yy_scan_string(cnf);
  	yyparse();

    Optimizer optimizer(finalFunction, tables, boolean, groupingAtts, attsToSelect, distinctAtts, distinctFunc, stats);

    QueryPlan *queryplan = optimizer.OptimizedQueryPlan();

    int ctr=0;
    while(tables)
    {
      tables = tables->next;
      ctr+=1;
    }
    cout<<"\nExpected number of tables for processing : 3\n";
    cout<<"\nActual number of tables processed : "<<ctr;

    ASSERT_EQ( ctr,3 );
}


TEST( QueryPlanTest, ParseTreeTest)
{
    // FILE *cat_fp = fopen("location", "r");
    // fscanf(cat_fp, "%s", catalog_path);
     Statistics *stats = new Statistics();
     stats->initStatistics();
     char *cnf = "SELECT SUM (n.n_regionkey) FROM nation AS n, region AS r WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_name = 'UNITED STATES') GROUP BY n.n_regionkey";

   	yy_scan_string(cnf);
   	yyparse();

    Optimizer optimizer(finalFunction, tables, boolean, groupingAtts, attsToSelect, distinctAtts, distinctFunc, stats);

    QueryPlan *queryplan = optimizer.OptimizedQueryPlan();
    QueryPlanNode *root = queryplan->root;

    int value = GetCount(root);
    queryplan->PrintInOrder();
    cout<<"\nExpected number of nodes in parse tree : 4";
    cout<<"\nActual number of nodes in parse tree : "<<value<<endl;

    ASSERT_EQ(value,4);
}




int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc,argv);
    return RUN_ALL_TESTS();
}
