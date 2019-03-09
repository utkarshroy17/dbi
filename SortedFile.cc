#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "SortedFile.h"
#include "BigQ.h"
#include <iostream>
#include <fstream>

#define PIPE_SIZE 100

// stub file .. replace it with your own DBFile.cc

Record temp1;

void *consumer(void *args) {

	oututil *o = (oututil *)args;
	o->inpipe->ShutDown();

	ComparisonEngine ceng;

	int err = 0;
	int i = 0;

	Record rec[2];

	Record tempRec;
	Record *last = NULL, *prev = NULL;

	while (o->outpipe->Remove(&rec[i % 2])) {
		prev = last;
		last = &rec[i % 2];

		if (prev && last) {
			if (ceng.Compare(prev, last, o->order) == 1) {
				err++;
			}
		}
		i++;
	}

}

void SortedFile::MergeOutputPipeToFile(){	//TODO: Complete

}

void SortedFile::ChangeReadToWrite(){
	if(m == READ){
		m = WRITE;
		input = new Pipe(PIPE_SIZE);
		output = new Pipe(PIPE_SIZE);
		bq = new BigQ(*input, *output, sortorder, runLength);
	}
}

void SortedFile::ChangeWriteToRead(){
	if(m == WRITE){
		m = READ;
		MergeOutputPipeToFile();
		MoveFirst();
	}
}

SortedFile::SortedFile(){
	curPageIndex = 0;
	pageFlag = 0;
	pageNumber = 0;
	m = READ;
		// input = new Pipe(100);
		// output = new Pipe(100);
}

int SortedFile::Create(char *f_path, fType f_type, SortInfo *startup) {

	//Create .bin file. Open if already created

	currFile.Open(0, f_path);
	return 1;
}

void SortedFile::Load(Schema &f_schema, const char *loadpath) {

	// Load the data from the .tbl file to the .bin file
	cout << loadpath << endl;
	FILE *tableFile = fopen(loadpath, "rb");

	Record temp;

	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {

		isSpaceEmpty = currPage.Append(&temp);

		if (!isSpaceEmpty) {
			currFile.AddPage(&currPage, pageNumber);
			currPage.EmptyItOut();
			currPage.Append(&temp);
			pageNumber++;
		}
	}

	currFile.AddPage(&currPage, pageNumber);
	pageNumber++;
}

int SortedFile::Open(char *f_path) {
	//Open .bin file

	currFile.Open(1, f_path);
	return 1;
}

void SortedFile::MoveFirst() {

	//Move the pointer to the first record in the file
	if (currFile.GetLength() - 1 < 0) {
		cout << "File is empty" << endl;
		return;
	}

	currFile.GetPage(&currPage, curPageIndex);
	currFile.MoveFirst();
	curPageIndex++;
	pageFlag = 1;
}

int SortedFile::Close() {

	cout << "\n Calling SortedFile CLOSE \n ";
	ChangeWriteToRead();
	//Close an opened File

	// if (m == WRITE) {
	// 	oututil o = { &input, &output, &sortorder };


	// 	BigQ bq(input, output, sortorder, 2);

	// }

	// return currFile.Close();
}


	
void SortedFile::Add(Record &rec) {	
	
	cout << "Adding record to input pipe in SortedFile \n";
	ChangeReadToWrite();
	
	// char *region = "partsupp";
	// char *catalog_path = "catalog";
	// Schema *testSchema = new Schema(catalog_path, region);	//TODO: this is hardcoded to religion. Change it
	// rec.Print(testSchema);
	
	input->Insert(&rec);	//TODO: uncomment this
}

int SortedFile::GetNext(Record &fetchme) {

	//Get first page
	if (pageFlag == 0) {
		currFile.GetPage(&currPage, curPageIndex);
		curPageIndex++;
		pageFlag = 1;
	}

	//Get the next pages
	if (currPage.GetFirst(&fetchme) == 1)
		return 1;
	else {
		if (curPageIndex > currFile.GetLength() - 2)
			return 0;
		else {
			currFile.GetPage(&currPage, curPageIndex);
			curPageIndex++;
			pageFlag++;
			currPage.GetFirst(&fetchme);
			return 1;
		}
	}
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

	ComparisonEngine comp;

	while (GetNext(fetchme)) {
		if (comp.Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}
	}

	return 0;
}
