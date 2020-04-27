#include "BigQ.h"

/*

void BigQ::sort_run(Page *p, int num_recs,File& new_file,int& gp_index,OrderMaker *sort_order){


	//cout<<"G index start "<<gp_index<<"\n";


	Record **record_Buffer= new Record*[num_recs];

	int c   =  0;
	int p_i =  0;
				

	do{	


		record_Buffer[c] = new Record();
				
				
		if((p+p_i)->GetFirst(record_Buffer[c])==0){
						
			(p+p_i)->EmptyItOut();	
			p_i++;
			(p+p_i)->GetFirst(record_Buffer[c]);
			c++;

		}
		else{
			c++;
		}
		

	}while(c < num_recs);

	(p+p_i)->EmptyItOut();

	sort(record_Buffer,record_Buffer+(num_recs),sort_func(sort_order));

*/






PQComparison :: PQComparison(OrderMaker *order)
{
        this->order=order;
}

bool PQComparison::operator ()( RunRecord* left, RunRecord* right) const
{
        ComparisonEngine engine;
        if (engine.Compare (left->record, right->record, order) >= 0) 
                return true;
        else
                return false;
}


CompareTwoRecords :: CompareTwoRecords(OrderMaker *order){
        this->order=order;
}

bool CompareTwoRecords::operator ()( Record* left, Record* right) const
{
        ComparisonEngine engine;

        if (engine.Compare (left, right, order) < 0) 
                return true;
        else
                return false;
}

void *StartOperation(void* arg) 
{
	vector<Record*> recStr;
	File file;
	Page pg;
	off_t pageIndex = 0;
	BigQStruct *bigQ = (BigQStruct *) arg;
	static int bufferCount;
	char mytempfile[100];
	RecordPQ runPQ(PQComparison(bigQ->order));
	sprintf(mytempfile,"buffer%d.tmp",bufferCount++);
	CompareTwoRecords recCompare(bigQ->order);	
	pg.EmptyItOut();
	Record temp, *rec;
	int counter = 0, popped = 0, pushed = 0;
	file.Open(0, mytempfile);
	int max_size = bigQ->runlen * PAGE_SIZE;
	int curSizeInBytes = sizeof(int);
	int recSize = 0;
	vector<int> pageIndices;
	int runCount = 0;

	while (bigQ->inPipe->Remove(&temp) == 1) 
	{
		recSize = (&temp)->GetSize(); 
		Record *newrec = new Record;
		newrec->Consume(&temp);
		if (curSizeInBytes + recSize <= max_size) 
		{
			recStr.push_back(newrec);
			pushed++;
			curSizeInBytes += recSize;
		}
		else 
		{
			runCount++;
			pageIndices.push_back(pageIndex);
			sort(recStr.begin(), recStr.end(), recCompare);
			for (int i = 0; i < recStr.size(); i++) 
			{
				rec = recStr.at(i);
				if (pg.Append(rec) == 0) 
				{
					file.AddPage(&pg, pageIndex++);
					pg.EmptyItOut();
					pg.Append(rec);
				}
				delete rec;
			}
			recStr.clear();
			if (pg.getCurSizeInBytes() > 0) 
			{
				file.AddPage(&pg, pageIndex++);
				pg.EmptyItOut();
			}
			recStr.push_back(newrec);
			curSizeInBytes = sizeof(int) + recSize;
		}
	}
    sort(recStr.begin(), recStr.end(), recCompare);
    pageIndices.push_back(pageIndex);
	for (int i = 0; i < recStr.size(); i++) 
	{
		rec = recStr.at(i);
		if (pg.Append(rec) == 0) 
		{
			file.AddPage(&pg, pageIndex++); 
			pg.EmptyItOut(); 
			pg.Append(rec);
		}
		delete rec;
	}
    recStr.clear();
    if (pg.getCurSizeInBytes() > 0) 
    {
		file.AddPage(&pg, pageIndex++);
        pg.EmptyItOut();
    }
    
    pageIndices.push_back(pageIndex);
    int numOfRuns = pageIndices.size() - 1;
    Run *runs[numOfRuns];
    for (int i = 0; i < numOfRuns; i++) 
    {
		Record* tmprec = new Record;
        runs[i] = new Run(&file, pageIndices[i], pageIndices[i + 1]);
        runs[i]->GetNext(tmprec);
        RunRecord* runRecord = new RunRecord(tmprec, runs[i]);
        runPQ.push(runRecord);
    }
    RunRecord *popPQ;
    Record* popRec;
    Run* popRun;
    while (!runPQ.empty()) 
    {
        popPQ = runPQ.top();
        runPQ.pop();
        popRun = popPQ->run;
        popRec = popPQ->record;
        bigQ->outPipe->Insert(popRec);
        delete popRec;
        Record* nextRecord = new Record;
        if (popRun->GetNext(nextRecord) == 1) 
        {
			popPQ->record = nextRecord;
			runPQ.push(popPQ);
		} 
		else 
		{
			delete popRun;
			delete popPQ;
			delete nextRecord;
		}
    }
    bigQ->outPipe->ShutDown();
    file.Close();
    remove(mytempfile);
}

/*

	int flags  = num_runs;
	int next  = 0;
	int start = 0;
	int end   = 1;
	int fin[num_runs];//set to 0
	int c_i[num_runs];	
	int index[num_runs][2];


	for(int i=0;i<num_runs;i++){
	
		fin[i]=0;
		c_i[i]=0;

	}
	
	for(int i=0;i<num_runs-1;i++){

		index[i][start] = 1+((*(args->run_length))*i);
		index[i][end]   = (*(args->run_length))*(i+1);
	}

	index[num_runs-1][start] = 1+((*(args->run_length))*(num_runs-1));
	index[num_runs-1][end] = gp_index-1;

	//DEBUG
		
	for(int i=0;i<num_runs;i++){

		//cout<<"run "<<i<<" start "<<index[i][start]<<" end "<<index[i][end]<<"\n";	
	
	}	


	//DEBUG

	//cout<<"Begin Merge\n";
	while(flags!=0){

		//Schema schema("catalog","lineitem");

		rwrap *temp;
		temp = pQueue.top();
		pQueue.pop();

		next = temp->run;

		//temp->rec.Print(&schema);

		args->output->Insert(&(temp->rec));

		rwrap *insert = new rwrap;		
		Record *t = new Record();
 

		
	//	(pQueue.top())->Print(&schema);



		if(fin[next]==0)
		{
			if((buf+next)->GetFirst(t)==0){
			
				if( index[next][start]+ (++c_i[next]) > index[next][end]){
				//what do you insert?
					flags--;
					fin[next]=1;				
				}
				else{

					//cout<<"Read at index"<<index[next][start]+c_i[next]<<"\n";

					new_file.GetPage(buf+next,(off_t)(index[next][start]+c_i[next]));

					(buf+next)->GetFirst(t);
					insert->rec = *t;
					insert->run = next;

					//insert->rec.Print(&schema);
 	
					pQueue.push(insert);
					
				}

			}
			else{

				insert->rec = *t;
				insert->run = next; 	

				//insert->rec.Print(&schema);		

				pQueue.push(insert);

			}	
		}	

		
		//delete insert;   	DOUBLE FREE ERROR
		//delete t;		DOUBLE FREE ERROR
	
	
	}

	while(!pQueue.empty()){
		args->output->Insert(&((pQueue.top())->rec));
		pQueue.pop();
	} 
	

	//pthread_exit();
	//cout<<"thread shutting down";
	//args->output->ShutDown();
}


*/




BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) 
{
        pthread_t myThread;
        BigQStruct *bqstruct = new BigQStruct{&in, &out, &sortorder, runlen};
        pthread_create(&myThread, NULL, StartOperation, bqstruct);

}

BigQ::~BigQ() {
}
