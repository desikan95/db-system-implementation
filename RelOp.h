#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <malloc.h>
#include <fstream>
#include <stdio.h>
#include <iostream>
#include <math.h>
#include <string.h>
#include <sstream>

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp {

	private:
		pthread_t worker_thread;
		DBFile *dbfile;
		Pipe *output;
		CNF *cnf;
		int n;
		Record *literal;

	public:
		void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void RunOperation();
		void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {

	private:
		pthread_t worker_thread;
		Pipe *input;
		Pipe *output;
		CNF *cnf;
		int n;
		Record *literal;

	public:
		void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void RunOperation();
		void Use_n_Pages (int n);
};

class Project : public RelationalOp
{
	private:
	Pipe *input;
	Pipe *output;
	int *keepMe;
	int numAttsInput, numAttsOutput;
	int run_length;
	pthread_t thread;


	public:
	Project(){};
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOperationProject();
	~Project(){};
};

class WriteOut : public RelationalOp
{
	private:
	Pipe *inPipe;
	FILE *outfile;
	Schema *schema;
	int run_length;
	pthread_t thread;

	public:
	WriteOut() {};
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOperationWriteOut();
	~WriteOut() {};
};

class DuplicateRemoval : public RelationalOp {
	private:
		pthread_t worker_thread;
		Pipe *input;
		Pipe *output;
		Schema *schema;
		int run_len;
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
		void WaitUntilDone ();
		void RunOperation();
		void Use_n_Pages (int n);
};

class Sum : public RelationalOp
{
	private:
	Pipe *inPipe;
	Pipe *outPipe;
	Function *compute;
	pthread_t thread;
	int run_length;

	public:
	Sum() {};
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOperationSum();
	~Sum() {};
};

class GroupBy : public RelationalOp
{
	private:
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *sortorder;
	Function *computeMe;
	int run_length;
	pthread_t thread;

	public:
	GroupBy() {};
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOperationGroupBy();
	~GroupBy() {};
};

class Join : public RelationalOp
{
	private:
	Pipe *inPipeLeft;
	Pipe *inPipeRight;
	Pipe *outPipe;
	CNF *cnf;
	Record *newRecord;
	int run_length;
	pthread_t thread;

	public:
	Join() {};
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
	void StartOperationJoin();
	~Join() {};
};

#endif
