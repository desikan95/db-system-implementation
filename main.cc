
#include <iostream>
#include <fstream>
#include "Defs.h"
#include "Optimizer.h"
#include "QueryPlan.h"
#include "ParseTree.h"
#include <time.h>

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
}

//Create table
extern struct CreateTable *createTable;
extern struct InsertFile *insertFile;
extern char *dropTableName;
extern char *setOutPut;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern int quit;

char catalog_path[50];
char dbfile_dir[50];
char tpch_dir[50];
int RUNLEN;

int main() {

	
	FILE *cat_fp = fopen("location", "r");
	fscanf(cat_fp, "%s", catalog_path);
	fscanf(cat_fp, "%s", dbfile_dir);
	fscanf(cat_fp, "%s", tpch_dir);
	fscanf(cat_fp, "%d", &RUNLEN);
	fclose(cat_fp);

	cout <<"sql->";
	cout.flush();
	yyparse();

	QueryPlan *queryPlan = new QueryPlan();
	if(quit) {
		cout <<"yhsql: Thanks for using yhsql. Bye!"<<endl;
		return 0;
	}
	//retrive the output target from output_path

	if(createTable){
		if(queryPlan->ExecuteCreateTable(createTable)) {
			cout <<"Created table"<<createTable->tableName<<endl;
		}
	}else if(insertFile) {
		if(queryPlan->ExecuteInsertFile(insertFile)) {
			cout <<"Loaded file "<<insertFile->fileName<<" into " <<insertFile->tableName<<endl;
		}
	} else if(dropTableName) {
		if(queryPlan->ExecuteDropTable(dropTableName)) {
			cout <<"Dropped dbfile"<<dropTableName<<endl;
		}
	} else if(setOutPut) {
		queryPlan->output = setOutPut;
		FILE *wfp = fopen(output_path, "w");
		fprintf(wfp, "%s", setOutPut);
		fclose(wfp);
		cout <<"Setted output to "<<setOutPut<<endl;
	} else if(tables){ // query
	//now we have all the info in the above data structure
		Statistics *s = new Statistics();
		s->initStatistics();

		Optimizer optimizer(finalFunction, tables, boolean, groupingAtts,
						attsToSelect, distinctAtts, distinctFunc, s);

		QueryPlan *queryPlan =
				optimizer.OptimizedQueryPlan();
		if(queryPlan == NULL) {
			cerr <<"ERROR in building query Plan!"<<endl;
			exit(0);
		}

		time_t t1;
		time(&t1);
		queryPlan->ExecuteQueryPlan();
		time_t t2;
		time(&t2);
		cout <<"Execution took "<<difftime(t2, t1)<<" seconds!"<<endl;
	}

	return 0;
}


