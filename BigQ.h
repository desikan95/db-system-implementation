#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <queue>
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <algorithm>
#include <ctime>
#include "Pipe.h"
#include "File.h"
#include "Record.h"


using namespace std;

typedef struct {
Pipe *inPipe;
Pipe *outPipe;
OrderMaker *order;
int runlen;
}BigQStruct;
        
class CompareTwoRecords {

private:
        OrderMaker *order;

public:
        CompareTwoRecords(OrderMaker *order);
        bool operator() ( Record *left, Record  *right) const;
};


class PQComparison {

private:
        OrderMaker *order;

public:
        PQComparison(OrderMaker *order);
        bool operator() ( RunRecord*  left, RunRecord* right) const;
};

typedef priority_queue<RunRecord* , vector<RunRecord*> ,PQComparison> RecordPQ ;


class BigQ {
        
public:
        

        friend void *TPMMS ( void* arg);
        BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
        ~BigQ ();
        
};


/*


public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

typedef struct rwrap{
	
	Record rec;
	int run;

}rwrap;


class sort_func{

private:

	OrderMaker *sort_order;

public:

	sort_func(OrderMaker *order){
		this->sort_order = order;
	}
*/


#endif
