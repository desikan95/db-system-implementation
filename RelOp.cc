#include "RelOp.h"

using namespace std;

void* startSelectFile(void* arg)
{
    SelectFile *sf = (SelectFile *)arg;
    sf->RunOperation();
}

void SelectFile::RunOperation()
{
    ComparisonEngine comp;
    Record rec;

    while ( dbfile->GetNext(rec, *cnf, *literal)==1 ) {	//Checking if each record matches the CNF
        output->Insert(&rec);														//Inserts only those records into the output
  	}
		output->ShutDown();
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->dbfile = &inFile;
    this->output = &outPipe;
    this->cnf = &selOp;
    this->literal = &literal;
		pthread_create(&worker_thread, NULL, startSelectFile, (void *)this);//Create a worker thread for the operation to run
}

void SelectFile::WaitUntilDone () {
	  pthread_join (worker_thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
    this->n = runlen;
}

void* startSelectPipe(void *arg)
{
    SelectPipe *sp = (SelectPipe *)arg;
    sp->RunOperation();

}

void SelectPipe::RunOperation()
{
    ComparisonEngine comp;
    Record rec;
    while(input->Remove(&rec))
    {
      if (comp.Compare(&rec,literal,cnf)==1)			//Checking if the records from the pipe match the CNF of the literal record
        output->Insert(&rec);											//Insert thos selected records into output pipe
    }
    output->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

    this->input = &inPipe;
    this->output = &outPipe;
    this->cnf = &selOp;
    this->literal = &literal;
    pthread_create(&worker_thread, NULL, startSelectPipe, (void *)this);	//Create worker thread for SelectPipe operation

}

void SelectPipe::WaitUntilDone () {
	   pthread_join (worker_thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
    this->n = runlen;
}



/* PROJECT OPERATION  */

void *projectThread(void *args){
	Project *p = (Project *)args;

	p -> StartOperationProject();
}

void Project :: StartOperationProject(){
	Record temRecord;
	int ctr = 0;
	while(input->Remove(&temRecord)){
		temRecord.Project(keepMe, numAttsOutput, numAttsInput);
		output->Insert(&temRecord);
	}
	input->ShutDown();
	output->ShutDown();
}

void Project ::  Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
	this->input = &inPipe;
	this->output = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	this->run_length = 1;
	pthread_create(&thread, NULL, projectThread, (void *)this);
}

void Project :: WaitUntilDone(){
	pthread_join(thread, NULL);
}

void Project :: Use_n_Pages(int n){
	this->run_length = n;
}


/* WRITE OUT OPERATION   */

void *writeOutThread(void *args){
	WriteOut *w = (WriteOut *)args;
	w->StartOperationWriteOut();
}

void WriteOut :: StartOperationWriteOut(){
	Record temRecord;
	while(inPipe->Remove(&temRecord))
  {
    temRecord.Print(schema);
    cout<<"\nWriting to a file dude\n";
		temRecord.WriteToFile(outfile, schema);			//Writing results out to file

  }
  fclose(outfile);
	inPipe->ShutDown();
}

void WriteOut :: Run(Pipe &inPipe, FILE *outFile, Schema &mySchema){
		this->inPipe = &inPipe;
		this->outfile = outFile;
		this->schema = &mySchema;
		pthread_create(&thread, NULL, writeOutThread, (void *)this);
}

void WriteOut :: WaitUntilDone(){
		pthread_join(thread, NULL);
}

void WriteOut :: Use_n_Pages(int n){
		this->run_length = n;
}


/*   DUPLICATE REMOVAL OPERATION   */

void* startDuplicateRemoval(void* arg){
    DuplicateRemoval *d = (DuplicateRemoval *)arg;
    d->RunOperation();
}

void DuplicateRemoval::RunOperation(){
    Pipe *sortedPipeData = new Pipe(100);
    OrderMaker *sortorder = new OrderMaker(schema);
    ComparisonEngine comp;
    BigQ bigq(*input,*sortedPipeData,*sortorder,10);
    Record curr_rec,next_rec;
    sortedPipeData->Remove(&next_rec);
		curr_rec.Copy(&next_rec);
    output->Insert(&next_rec);
    while(sortedPipeData->Remove(&next_rec) == 1){
      if (comp.Compare(&curr_rec,&next_rec,sortorder) != 0){			//Checking consectuive records to see if they match																													//If records are distinct then we push them to output pipe
        curr_rec.Copy(&next_rec);																//Copy next record into current record
				output->Insert(&next_rec);
      }
    }
		input->ShutDown();
    output->ShutDown();
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
    this->input = &inPipe;
    this->output = &outPipe;
    this->schema = &mySchema;
    pthread_create(&worker_thread, NULL, startDuplicateRemoval, (void *)this);
}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(worker_thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n){
    this->run_len = n;
}

/* SUM OPERATION   */

void *sumThread(void *arg){
	Sum *s = (Sum *) arg;
	s->StartOperationSum();
}

void Sum :: StartOperationSum(){
	Record temRecord;
	Record rec;
	Type type;
	int IntRecSum =0;
	int IntSumTotal = 0;
	double DblSumRec = 0.0;
	double DblSumTotal = 0.0;
	while(inPipe->Remove(&rec)){
		int checker = compute->Apply(rec, IntRecSum, DblSumRec);

    cout<<"\nCHECKER IS "<<checker;
		if(checker == 0){
			type = Int;
			IntSumTotal +=  IntRecSum;				//Computing the sum by processing each record
		}
		else{
			type = Double;
			DblSumTotal += DblSumRec;
		}
	}
  cout<<"\nINTSUM"<<IntSumTotal<<endl;

  ostringstream result;
  string resultSum;
  Record resultRec;

  // create output record
  if (type == Int) {

          result << IntSumTotal;
          resultSum = result.str();
          resultSum.append("|");

          Attribute IA = {"int", Int};
          Schema out_sch("out_sch", 1, &IA);
          resultRec.ComposeRecord(&out_sch, resultSum.c_str());

  } else {

          result << DblSumTotal;
          resultSum = result.str();
          resultSum.append("|");

          Attribute DA = {"double", Double};
          Schema out_sch("out_sch", 1, &DA);
          resultRec.ComposeRecord(&out_sch, resultSum.c_str());
  }
  Attribute IA = {"int", Int};
  Schema out_sch("out_sch", 1, &IA);
  cout<<"\nPRINTING RECORD>>>>>>>>>>>>>>";
  resultRec.Print(&out_sch);


//	temRecord.ComposeRecord(type, IntSumTotal, DblSumTotal);		//Putting the aggregate sum into a record
	outPipe->Insert(&resultRec);
	outPipe->ShutDown();
}

void Sum :: Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->compute = &computeMe;
	pthread_create(&thread, NULL, sumThread, (void *)this);
}

void Sum :: WaitUntilDone(){
	pthread_join(thread, NULL);
}

void Sum :: Use_n_Pages(int n){
	this->run_length = n;
}


/* GROUPBY OPERATION  */

void *groupByThread(void *arg){
	GroupBy *g = (GroupBy *)arg;
	g->StartOperationGroupBy();
}

void GroupBy :: StartOperationGroupBy()
{
	Record *begin = NULL;
	Record *finish = NULL;
	Record *temRecord = new Record;
	Record *sortRec = new Record;
	Record recArr[2];
	Type type;
	int buff_size = 100;
	Pipe *pipe = new Pipe(buff_size);

	ComparisonEngine comp;
	int arrbound = (sortorder->numAtts)+1;
	int groupNumber = 0;
	int intSum = 0;
	int intRec = 0;
	int attsArr[arrbound];
	double dblSum = 0.0;
	double dblRec = 0.0;
	/* Read the tuple into memory M blocks at a time and sort each block*/
	attsArr[0] = 0;
	for(int i = 1; i < arrbound; i++)
		attsArr[i] = sortorder->whichAtts[i-1];

	BigQ bigq(*inPipe, *pipe, *sortorder, run_length); //sort using the big class
	//Reading the tuple into memory and sorting them based on the attribute
	while(pipe->Remove(&recArr[groupNumber%2]) == 1){
		begin = finish;
		finish = &recArr[groupNumber%2];
		if(begin != NULL && finish != NULL){
			if(comp.Compare(begin, finish, sortorder) != 0){
				computeMe->Apply(*begin, intRec, dblRec);
				if(computeMe->returnsInt == 1){
					type = Int;
					intSum += intRec;
				}
				else{
					type = Double;
					dblSum += dblRec;
				}
				//Get the starting of the sublist
				int startint = ((int *)begin->bits)[1]/sizeof(int) - 1;
				//Create record based on the sublist
				sortRec->ComposeRecord(type, intSum, dblSum);
				//merge the recordds
				temRecord->MergeRecords(sortRec, begin, 1, startint, attsArr, arrbound, 1);
				//append to output file
				outPipe->Insert(temRecord);
				intSum = 0;
				dblSum = 0.0;
			}
			else{
				//if sort order is not present, simply compute the sum
				computeMe->Apply(*begin, intRec, dblRec);
				if(computeMe->returnsInt == 1){
					type = Int;
					intSum += intRec;
				}
				else{
					type = Double;
					dblSum += dblRec;
				}
			}
		}
		groupNumber++;
	}
	//finally append the final sublist
	computeMe->Apply(*finish, intRec, dblSum);
	if(computeMe->returnsInt == 1){
		type = Int;
		intSum += intRec;
	}
	else{
		type = Double;
		dblSum += dblRec;
	}
	int startint = ((int *)begin->bits)[1]/sizeof(int) - 1;
	sortRec->ComposeRecord(type, intSum, dblSum);
	temRecord->MergeRecords(sortRec, finish, 1, startint, attsArr, arrbound, 1);
	outPipe->Insert(temRecord);

	outPipe->ShutDown();
}

void GroupBy :: Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->sortorder = &groupAtts;
	this->computeMe = &computeMe;
	pthread_create(&thread, NULL, groupByThread, (void *)this);
}

void GroupBy :: WaitUntilDone(){
	pthread_join(thread, NULL);
}

void GroupBy :: Use_n_Pages(int n){
	this->run_length = n;
}


/*Join Operation   */

void *SetupThreadJoin(void *args){
	Join *j = (Join *) args;
	j->StartOperationJoin();
}

void Join :: StartOperationJoin(){
	int leftint, rightint;
	int buffer_size = 100;
	Record *recordLeft = new Record();
	Record *recordRight = new Record();
	Record *recordJoin = new Record();
	ComparisonEngine compare;
	Pipe *l_pipe = new Pipe(buffer_size); //pipe for left table
	Pipe *r_pipe = new Pipe(buffer_size); //pipe for right table
	OrderMaker l_sortOrder;
	OrderMaker r_sortOrder;
	vector<Record *> leftVector, rightVector;

	if(cnf->GetSortOrders(l_sortOrder, r_sortOrder)){ //get the sort order for both tuples

		//sort the left and right tuples
		BigQ leftBigq(*inPipeLeft, *l_pipe, l_sortOrder, 10);
		BigQ rightBigq(*inPipeRight, *r_pipe, r_sortOrder, 10);

		//take the top record from both pipes
		l_pipe->Remove(recordLeft);
		r_pipe->Remove(recordRight);

		//set the flags for quit condition
		bool lFlag = false;
		bool rFlag = false;

		//get the size of the record
		leftint = ((int *) recordLeft->bits)[1] / sizeof(int) -1;
		rightint = ((int *) recordRight->bits)[1] / sizeof(int) -1;

		int totalint = leftint + rightint;
		int attsArr[totalint];
		for(int i=0; i<leftint; i++)
			attsArr[i] = i;
		for(int i=0; i<rightint; i++)
			attsArr[leftint+i] = i;

		while(true){
			if(compare.Compare(recordLeft, &l_sortOrder, recordRight, &r_sortOrder) < 0){
				if(l_pipe->Remove(recordLeft) != 1) //if left pipe is empty
				break;
			}
			else if(compare.Compare(recordLeft, &l_sortOrder, recordRight, &r_sortOrder) > 0){
				if(r_pipe->Remove(recordRight) != 1) //if right pipe is empty.
					break;
			}
			else{
				//if both pipes are not empty, set the flags to false
				lFlag = false;
				rFlag = false;

				while(true){ //Read the left pipe and store the records in left vector
					Record *dispRecord = new Record;
					dispRecord->Consume(recordLeft);
					leftVector.push_back(dispRecord);
					if(l_pipe->Remove(recordLeft) != 1){
						lFlag = true;
						break;
					}
					if(compare.Compare(dispRecord, recordLeft, &l_sortOrder) != 0)
						break;
				}
				while(true){ //read the right pipe and store the records in right vector
					Record *dispRecord = new Record;
					dispRecord->Consume(recordRight);
					rightVector.push_back(dispRecord);
					if(r_pipe->Remove(recordRight) != 1){
						rFlag = true;
						break;
					}
					if(compare.Compare(dispRecord, recordRight, &r_sortOrder) != 0)
						break;
				}

				//use the right and left vector and perform the merge operation
				for(int i = 0; i < leftVector.size(); i++){
					for(int j = 0; j < rightVector.size(); j++){
						recordJoin->MergeRecords(leftVector.at(i), rightVector.at(j), leftint, rightint, attsArr, leftint+rightint, leftint);
						outPipe->Insert(recordJoin);
					}
				}

				// clear the vectors
				leftVector.clear();
				rightVector.clear();

				if(lFlag || rFlag) //if both pipes are empty then exit
					break;
			}
		}
	}
	//if sort order is not present
	else{
		//store all the values of right record into right vector
		while(inPipeRight->Remove(recordRight)){
			Record temRecord;
			temRecord.Consume(recordRight);
			rightVector.push_back(&temRecord);
		}
		//get the top record from left pipe
		inPipeLeft->Remove(recordLeft);
		//get the size of left record and right vector. Will be used during merge step
		leftint = ((int *) recordLeft->bits)[1] / sizeof(int) -1;
		rightint = ((int *) rightVector.at(0)->bits)[1] / sizeof(int) -1;

		int attsArr[leftint+rightint];

		//get the attributes and store it in array
		for(int i=0; i<leftint; i++)
			attsArr[i] = i;
		for(int i=0; i<rightint; i++)
			attsArr[leftint+i] = i;
		//for every left record perform join operation merging the left records with the correct right record.
		while(inPipeLeft->Remove(recordLeft)){
			for(int j = 0; j < rightVector.size(); j++){
				recordJoin->MergeRecords(recordLeft, rightVector.at(j), leftint, rightint, attsArr, leftint+rightint, leftint);
				outPipe->Insert(recordJoin);
			}
		}
	}
	outPipe->ShutDown(); // shutdown the pipe
}

void Join :: Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inPipeLeft = &inPipeL;
	this->inPipeRight = &inPipeR;
	this->outPipe = &outPipe;
	this->cnf = &selOp;
	this->newRecord = &literal;
	pthread_create(&thread, NULL, SetupThreadJoin, (void *)this);
}

void Join :: WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Join :: Use_n_Pages(int n)
{
	this->run_length = n;
}
