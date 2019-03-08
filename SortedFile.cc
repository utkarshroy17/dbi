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

// stub file .. replace it with your own DBFile.cc

Record temp1;

void *producer(void *arg) {
	cout << " one" << endl;

	char *region = "region";
	char *catalog_path = "catalog";
	Schema *testSchema = new Schema(catalog_path, region);


	//cout << "produce" << endl;
	inutil *myArgs = (inutil *)arg;

	//Pipe *myPipe = (Pipe *)arg;
	cout << "rec print ";
	//temp1.Print(testSchema);

	//myPipe->Insert(&temp1);
	Record *dummy = &(myArgs->rec);
	dummy->Print(testSchema);
	//myArgs->pipe->Insert(myArgs->rec);
}

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

SortedFile::SortedFile(){
	curPageIndex = 0;
	pageFlag = 0;
	pageNumber = 0;
	m = read;
	input = new Pipe(100);
	output = new Pipe(100);
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
	//Close an opened File

	// if (m == write) {
	// 	oututil o = { &input, &output, &sortorder };

	// 	pthread_create(&thread2, NULL, consumer, (void*)&o);

	// 	BigQ bq(input, output, sortorder, 2);

	// 	pthread_join(thread1, NULL);
	// 	pthread_join(thread2, NULL);
	// }

	// return currFile.Close();
}


	
void SortedFile::Add(Record &rec) {	
	
	cout << "Calling Add, SortedFile.cc \n";

	Record *temp = new Record;
	// temp->Copy(&rec);
	
	if (m == read) {
		m = write;
		OrderMaker dummy; //TODO: replace with working orderMaker
		bq = new BigQ(*input, *output, dummy, 2);
	}
	
	cout << "Adding record to input pipe in SortedFile \n";

	// char *region = "region";
	// char *catalog_path = "catalog";
	// Schema *testSchema = new Schema(catalog_path, region);	//TODO: this is hardcoded to religion. Change it
	// temp->Print(testSchema);
	
	// input->Insert(temp);	//TODO: uncomment this
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
