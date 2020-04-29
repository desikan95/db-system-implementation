
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


	FILE *locationfile = fopen("location", "r");
	fscanf(locationfile, "%s", catalog_path);
	fscanf(locationfile, "%s", dbfile_dir);
	fscanf(locationfile, "%s", tpch_dir);
	fscanf(locationfile, "%d", &RUNLEN);
	fclose(locationfile);

	int option = 0;
	cout<<"\nHello, User. Welcome to our DB. This was developed by Desikan Sundararajan and Aditya Karlekar in C++.\n";
	cout<<"\nThe following functionalities have been implemented : \n";
	cout<<"1. Create Table\n2. Insert Into Table\n3. Drop Table\n4. Execute Query\n\n";

	do {

					cout<<"\nEnter your query below : ";
					cout <<"\n>>>";
					cout.flush();
					yyparse();


					QueryPlan *queryPlan = new QueryPlan();
					//retrive the output target from output_path

					if(createTable){
							if(queryPlan->ExecuteCreateTable(createTable))
							{
									cout<<createTable->tableName<<" has been created "<<endl;
									exit(1);
							}
					}
					else if(insertFile)
					{
							if(queryPlan->ExecuteInsertFile(insertFile))
							{
								cout <<"Loaded file : "<<insertFile->fileName<<" into " <<insertFile->tableName<<endl;
								exit(1);
							}
					}
					else if(dropTableName)
					{
							if(queryPlan->ExecuteDropTable(dropTableName))
							{
								cout <<"Dropped table "<<dropTableName<<endl;
								exit(1);
							}
					}
					else if(setOutPut)
					{
							queryPlan->output = setOutPut;
							FILE *wfp = fopen(output_path, "w");
							fprintf(wfp, "%s", setOutPut);
							fclose(wfp);
							cout <<"Output has been set to "<<setOutPut<<endl;
					}
					else if(tables)
					{
							Statistics *s = new Statistics();
							s->initStatistics();

							Optimizer optimizer(finalFunction, tables, boolean, groupingAtts,	attsToSelect, distinctAtts, distinctFunc, s);
							QueryPlan *queryPlan = optimizer.OptimizedQueryPlan();
							if(queryPlan == NULL) {
								cerr <<"ERROR in building query Plan!"<<endl;
								exit(0);
							}
							if( strcmp(queryPlan->output, "NONE") == 0)
								queryPlan->ExecuteQueryPlan();
							else
								queryPlan->ExecuteQuery();
							exit(1);
					}

					if (quit)
							option = 1;

	}
	while(option==0);

	return 0;
}
