#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "DBFile.h"


Heap::Heap(){
    file = new File();
    pageRead = new Page();
    pageWrite = new Page();
    currRecord = new Record();

}

Heap::~Heap(){
    delete file;
    delete pageRead;
    delete pageWrite;
    delete currRecord;
}
int Heap::Create (char *f_path, fType f_type, void *startup) {
    file->Open(0,f_path); //function to create the file. Open with parameter 0 creates the file
    pageNumber = 1;  //setting page number to 1
    writePageNumber = 1; // setting write page number for writing purpose
    writeMode = true;  // setting write mode to true
    eof = true;  // setting end of file to true
    return 1;
}

void Heap::Load (Schema &f_schema, const char *loadpath) {
  FILE* tablepath = fopen(loadpath, "r"); // file object to open the file
    Record record; // record object

    while(record.SuckNextRecord(&f_schema, tablepath) != 0){ // traversing through record
        Add(record); //adding the record
    }
  file->AddPage(pageWrite, pageNumber); //adding the page to file
    fclose(tablepath); //closing the file
}

int Heap::Open ( char *f_path) {

  try
  {
      file->Open(1,f_path); //Opening the file
      eof = false; // setting end of file to false
      writeMode = false; //setting write mode flag to false
      pageNumber = 1; // setting page number to 1
      return 1;
  }
  catch (...)
  {
      return 0;
  }

}

void Heap::MoveFirst () {
  // if (!pageRead.empty())
  //  theFile.addPage(&curPage);
  file->GetPage(pageRead,1); //Using GetPage function to reach the first page of file

//  currRecord->MoveToStart();

}

int Heap::Close () {

  //Check for dirty bit
  if(writeMode){ //Checking for writemode to be true
      file->AddPage(pageWrite, pageNumber);
  }

  eof = false;
  return file->Close(); // Closing the file
  return 1;
}

void Heap::Add (Record &rec) {
    writeMode = true;  // Setting write mode to true
  //Page limit not reached
  if (pageWrite->Append(&rec) == 0)
  {
    file->AddPage(pageWrite,pageNumber++);
    pageWrite->EmptyItOut();
    pageWrite->Append(&rec);
  }
}



int Heap::GetNext(Record &fetchme){

  if(pageRead->GetFirst(&fetchme) == 1){  // check whether first record exists
    return 1;
  }
  else{
    pageNumber++;                     //increment to next page
    if(pageNumber < file->GetLength() - 1){ // checking if pages are present
      file->GetPage(pageRead, pageNumber); // getting the page
      pageRead->GetFirst(&fetchme); // read the page
      return 1;
    }
    else{
      return 0; // return 0 in case of failure
    }
  }

}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  //cnf.GrowFromParseTree (final, fetchme, literal);

  ComparisonEngine comparison; //comaprison object
    while(true){
      if(pageRead->GetFirst(&fetchme) == 1){
        if(comparison.Compare(&fetchme, &literal, &cnf)){ // return 1 if the cnf satisfies the criterion
            return 1;
        }
      }
        else{
          pageNumber++; //increment to next page
          if(pageNumber < file->GetLength() - 1){ //if pages are avaiable; get the next page
            file->GetPage(pageRead, pageNumber);
          }
          else{
            return 0; // return 0 in case of failure
          }
        }


    }

}