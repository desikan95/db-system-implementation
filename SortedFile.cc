#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "DBFile.h"

// Sorted::Sorted(){
//     //input = new Pipe();
//     //output = new Pipe();
//     // dirty = false;
//     // file    = new File();
//     // buffersize = 100;
//     buffersize = 100;
//     bigq = NULL;
//     dirty = false;
//     file  = new File();

//     queryMake = false;
//     sortOrder = true;


// }

Sorted::Sorted(SortInfo *info){
    sortinfo = info;
    buffersize = 10000;
    bigq = NULL;
    dirty = false;
    file  = new File();
    input = new Pipe(buffersize);
    //input->Insert(&addme);
    output = new Pipe(buffersize);

    queryMake = false;
    sortOrder = true;


}

void Sorted :: initBigQ(){
    input = new Pipe(buffersize);
    output = new Pipe(buffersize);
     bigq = new BigQ(*input, *output, *(sortinfo->myOrder), sortinfo->runLength);
}

Sorted::~Sorted(){

}


void Sorted::Merge(){
  input->ShutDown();
  ComparisonEngine c;
  state=Read;
  Record *sortedData = new Record;
  Record *pipeData = new Record;
  DBFile tempFile;
  tempFile.Create("tempfile.tmp",heap,NULL);

  this->MoveFirst();

  int pipeStatus = output->Remove(sortedData);
  int sortedDataStatus = GetNext(*pipeData);

  while(pipeStatus==1 && sortedDataStatus==1)
  {
    if (c.Compare(sortedData,pipeData,sortinfo->myOrder)<=0)
    {
      tempFile.Add(*sortedData);
      pipeStatus = output->Remove(sortedData);
    }
    else
    {
      tempFile.Add(*pipeData);
      sortedDataStatus = GetNext(*pipeData);
    }
  }
  while(pipeStatus==1)
  {
    tempFile.Add(*sortedData);
    pipeStatus = output->Remove(sortedData);
  }
  while(sortedDataStatus==1)
  {
    tempFile.Add(*pipeData);
    sortedDataStatus = GetNext(*pipeData);
  }
  tempFile.Close();
  remove(fileName);
  rename("tempfile.tmp",fileName);
}

int Sorted :: Create ( char *f_path, fType file_type, void *startup){
    file->Open(0,f_path);
    pg.EmptyItOut();
	fileName = new char[strlen(f_path)];
	strcpy(fileName,f_path);
    dirty = false;
    sortinfo = (SortInfo*) startup;
    pageIndex = 0;
    recordIndex = 0;
    fileEnd = true;
    return 1;


}
	int Sorted :: Open (char *fpath){
        dirty = false;
        char name[50];
        // append the metadata tag
        sprintf(name, "%s.meta", fpath);
        fileName = new char[strlen(fpath)];
       // file
        strcpy(fileName, fpath);
        pg.EmptyItOut();

        ifstream inputStream(name, ios::binary);

        inputStream.seekg(sizeof(fileName) - 1); //the sortorder struct values is appended
            //at the end of the file. We seek till end to extract them

        if(!sortinfo){
            sortinfo = new SortInfo;
            sortinfo->myOrder = new OrderMaker(); //reconstituting the order maker

        }

        //read the myorder and runlength
        inputStream.read((char *)sortinfo->myOrder, sizeof(*(sortinfo->myOrder)));
        inputStream.read((char*)&(sortinfo->runLength), sizeof(sortinfo->runLength));
        inputStream.close();

        state = Read;

        file->Open(1, fpath);
        pageIndex = 0;
        recordIndex = 0;
        fileEnd = false;

    }
	int Sorted :: Close (){
        if(state == Write){
            Merge();
        }

        file->Close();
        dirty = false;
        fileEnd = true;

        //creating the meta-data file

        char name[50];
        sprintf(name, "%s.meta", fileName);

        ofstream out(name);
        out<<"sorted"<<endl;
        out.close();

        //appending ordermaker and runlength
        //used in open function

        ofstream output(name,ios::binary|ios::app); //append in binary form and at the end of file contents
        output.write((char *) sortinfo->myOrder, sizeof(*(sortinfo->myOrder)));
        output.write((char *)&(sortinfo->runLength), sizeof(sortinfo->runLength));

        output.close();

    }

	void Sorted :: Load (Schema &myschema, const char *loadpath){
            if (state == Read){
                state = Write;
                if(!bigq){
                    initBigQ();
                }
                pg.EmptyItOut();
                FILE* newFile = fopen(loadpath, "r");
                Record temp;

                while(temp.SuckNextRecord(&myschema, newFile) != 0){
                    input->Insert(&temp);
                }
                fclose(newFile);



            }
    }
	void Sorted :: MoveFirst (){

        cout<<"\nInside Move First";
        if(state == Write){
            Merge();
        }
        pageIndex = 0;
        fileEnd = false;
        pg.EmptyItOut();
        //pageRead->EmptyItOut();
        if(file->GetLength() > 0){
            file->GetPage(&pg, pageIndex);

        }
        queryMake = false;
        sortOrder = true;

    }


	void Sorted :: Add (Record &addme){

        cout<<"\nBeginning of add function";
      //  input->Insert(&addme);
        // if(state == Read){
        //     //input->Insert(&addme);
        //     if(bigq == NULL){
        //        // initBigQ();
        //
        //
        //         cout<<"Just before calling new thread";
        //
        //         bigq = new BigQ(*input, *output, *(sortinfo->myOrder), sortinfo->runLength);
        //
        //         //bigq = new BigQ(*input, *output, *(sortinfo->myOrder), sortinfo->runLength);
        //         state = Write;
        //     }
        //     input->Insert(&addme);

        if (state==Read){
            if(!bigq)
				        initBigQ();
			      state=Write;

      	// // thread to dump data into the input pipe (for BigQ's consumption)
      	// pthread_t thread1;
      	// pthread_create (&thread1, NULL, producer, (void *)&input);
      	// // thread to read sorted data from output pipe (dumped by BigQ)
      	// pthread_t thread2;
      	// pthread_create (&thread2, NULL, consumer, (void *)&output);
        //
      	// bigq= new BigQ(*input, *output, *(sortinfo->myOrder), sortinfo->runLength);
        //
      	// pthread_join (thread1, NULL);
      	// pthread_join (thread2, NULL);

      }
      input->Insert(&addme);




    }
	int Sorted :: GetNext (Record &rec){

        if(state == Write){
            Merge();
        }
        if(pg.GetFirst(&rec) == 1)
            return 1;
        else{
            ++pageIndex;
            if(pageIndex<file->GetLength() - 1){
                file->GetPage(&pg, pageIndex);
                pg.GetFirst(&rec);
                return 1;
            }
            else{
                return 0;
            }

        }


    }

int Sorted::GetNextSequential (Record &fetchme, CNF &cnf, Record &literal)
{
        ComparisonEngine comp;
        do
        {
      			if(pg.GetFirst(&fetchme)==1 )
      			{
      				if (comp.Compare (&fetchme, &literal, &cnf))
      					return 1;
      			}
      			else
      			{
      				pageIndex++;
      				if(pageIndex<file->GetLength()-1)
      					file->GetPage(&pg,pageIndex);
      				else
      					return 0;
      			}
        }
        while(true);
}

int Sorted :: GetNextHelper(Record &fetchme, CNF &cnf, Record &literal)
{
		ComparisonEngine comp;
		while(true)
		{
			if(pg.GetFirst(&fetchme)==1 )
			{
  				if(comp.Compare (&literal, orderquery, &fetchme,sortinfo->myOrder) ==0)   //Comparing literal record with order query attribute and current recod with sort_order
  				{                                                                           //attributes
    					if (comp.Compare (&fetchme, &literal, &cnf))
    						    return 1;
  				}
  				else
  					   return 0;
			}
			else                                                                         //Incrementing page if end of page has been reached
			{
  				pageIndex++;
  				if(pageIndex<file->GetLength()-1)
  					file->GetPage(&pg,pageIndex);
  				else
  					return 0;
			}
		}
}

int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
    		if(state==Write)
    		{
    			Merge();
    			queryMake=false;
    			sortOrder=true;
    		}

    		if(sortOrder) //If sort order exists, then do a binary search using OrderMaker query to find record quickly
    		{
      			if(!queryMake) //Make the query if it does not exist already
      			{
        				queryMake=true;
        				int found;
        				orderquery=new OrderMaker;
        				if(cnf.GetSortOrdersforQuery(*orderquery,*(sortinfo->myOrder))>0) //if order is possible
        				{
          					found=BinarySearch(fetchme,cnf,literal);
          					ComparisonEngine comp;
          					if(found)
          					{
            						if (comp.Compare (&fetchme, &literal, &cnf))
            							     return 1;
            						else
            						       GetNextHelper(fetchme, cnf, literal); //if it is equal, find the next record that matches the CNF

          					}
          					else
          						    return 0;
        				}
        				else  //if no order is possible based on a comparison of CNF and sort_order attributes
        				{
          					sortOrder=false;
          					return GetNextSequential(fetchme, cnf, literal);  //Do a sequential scan because no order is possible using CNF and sort_order attributes
        				}
      			}
      		  else
      				  GetNextHelper(fetchme, cnf, literal);  //if it is equal, find the next record that matches the CNF
    		}
    		else
    			  return GetNextSequential(fetchme, cnf, literal);  //Do a sequential scan because no order is possible using CNF and sort_order attributes
}

int Sorted::BinarySearch(Record& fetchme,CNF &cnf,Record &literal)  //This function is used for matching the CNF parsed to the sort order of the DBFile
{                                                                   //using a binary search of records
    		off_t start=pageIndex;
    		off_t end=file->GetLength()-2;
    		Page *midPage=new Page;
    		bool found=false;
    		ComparisonEngine comp;
    		off_t mid=start;
    		while(start < end)
    		{
      			mid=(start+end)/2;
      			file->GetPage(midPage,mid);
      			if(midPage->GetFirst(&fetchme)==1 )
      			{
      				if (comp.Compare (&literal, orderquery, &fetchme,sortinfo->myOrder) <= 0) //if literal is before fetchme in sorted order
      					end = mid;
      				else
      				{
      					start=mid;
      					if(start == end -1)
      					{
      						file->GetPage(midPage,end);
      						midPage->GetFirst(&fetchme);
      						if (comp.Compare (&literal, orderquery, &fetchme,sortinfo->myOrder) > 0) //if literal is after fetchme in sorted order
      							mid=end;
      						break;
      					}
      				}
      			}
      			else
      				break;
    		}
    		if(mid==pageIndex)
    		{
      			while(pg.GetFirst(&fetchme)==1)
      			{
      				if( comp.Compare (&literal, orderquery, &fetchme,sortinfo->myOrder) == 0 )
      				{
      					found=true;
      					break;
      				}
      			}
    		}
    		else
    		{
      			file->GetPage(&pg,mid);
      			while(pg.GetFirst(&fetchme)==1)
      			{
      				if( comp.Compare (&literal, orderquery, &fetchme,sortinfo->myOrder) == 0 )
      				{
      					found=true;
      					pageIndex=mid;
      					break;
      				}
      			}
    		}


        if(!found && mid < file->GetLength()-2)
        {
      			file->GetPage(&pg,mid+1);
      			if(pg.GetFirst(&fetchme)==1 && comp.Compare (&literal, orderquery, &fetchme,sortinfo->myOrder) == 0)
      			{
      				found=true;
      				pageIndex=mid+1;
      			}
    		}
    		if(!found)
    			return 0;
    		else
    			return 1;
}
