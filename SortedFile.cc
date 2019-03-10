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
#include "string.h"
#include <bits/stdc++.h> 

#define PIPE_SIZE 100

Record temp1;

void readMetaFile (char *f_path, OrderMaker *&o, int &rl) {

	char *metaFilename = new char[100];
	sprintf(metaFilename, "%s.header", f_path);
	cout << "Opening Meta File " << metaFilename << endl;
	string line;
	ifstream meta (metaFilename);
	getline(meta, line);
	cout << "line\n";

	if(line.compare("heap") == 0){
		cout << "heap";
	}if(line.compare("sorted") == 0){
		cout << "sorted \n";
		getline(meta, line);
		int runLength = stoi (line, nullptr, 10);
		cout << "runLength " << runLength << endl;
		getline(meta, line);
		int numAtts = stoi (line, nullptr, 10);
		cout << "numAtts " << numAtts << endl;
		
		int whichAtts[numAtts];
		Type whichTypes[numAtts];
		int i;
		string word;
		
		getline(meta, line);
		istringstream ss(line);
		for(i = 0; i < numAtts; i++){
			ss >> word;
			whichAtts[i] = stoi(word, nullptr, 10);
		}

		getline(meta, line);
		istringstream ss1(line);
		for(i = 0; i < numAtts; i++){
			ss1 >> word;
			if(word == "Int"){
				whichTypes[i] = Int;
			}else if(word == "Double"){
				whichTypes[i] = Double;
			}else {
				whichTypes[i] = String;
			}
		}

		OrderMaker *sortorder = new OrderMaker();
		sortorder->Set(numAtts, whichAtts, whichTypes);
		o = sortorder;
		rl = runLength;
		// dummy.Print();
		
		// cout << "whichAtts ";
		// for(int i = 0; i < numAtts; i++){
		// 	cout << whichAtts[i] << " ";
		// }
		// cout << endl;
		// cout << "whichTypes ";
		// for(i = 0; i < numAtts; i++){
		// 	if(whichTypes[i] == Int){
		// 		cout << "Int ";
		// 	}else if(whichTypes[i] == Double){
		// 		cout << "Double ";
		// 	}else {
		// 		cout << "String ";
		// 	}
		// }
		// cout << endl;

	}
	meta.close();
}

void SortedFile::MergeOutputPipeToFile() {	//TODO: Complete

	cout << "MergeOutputPipeToFile \n";

	Schema *testSchema = new Schema("catalog", "partsupp");

	input->ShutDown();

	//ComparisonEngine *ce;

	//toBeMerged = new Page();
	//Record *recFromFile = new Record();
	//pagePtrForMerge = 0;
	////GetNew(recFromFile);

	//Record *tempRec, *tempRecFromFile;
	//Record *getRec = new Record();


	//bool noMoreRecs = true;	
	//int result = GetNew(recFromFile);
	//int pageIndex = 1;
	//bool leftIsSmaller = false;

	//if (result == 0)
	//	noMoreRecs = true;
	
	/*while (!noMoreRecs) {

		if (output->Remove(getRec)) {

			tempRec = new Record;
			tempRec->Copy(getRec);

			if (ce->Compare(tempRec, recFromFile, si->order) < 0) {
				isPageEmpty = pageToWrite.Append(tempRec);
				leftIsSmaller = true;
			}
			else {
				isPageEmpty = pageToWrite.Append(recFromFile);
				leftIsSmaller = false;
			}
				

			if (isPageEmpty == 0) {
				newFile.AddPage(&pageToWrite, pageIndex++);

				pageToWrite.EmptyItOut();

				if(leftIsSmaller)
					pageToWrite.Append(tempRec);
				else
					pageToWrite.Append(recFromFile);
			}

			if (GetNew(recFromFile) == 0) {

				noMoreRecs == true;
				break;
			}
		}
		else {
			while (GetNew(recFromFile) != 0) {

				tempRecFromFile = new Record;
				tempRecFromFile->Copy(recFromFile);

				isPageEmpty = pageToWrite.Append(tempRecFromFile);

				if (isPageEmpty == 0) {

					newFile.AddPage(&pageToWrite, pageIndex++);

					pageToWrite.EmptyItOut();

					pageToWrite.Append(tempRecFromFile);
				}
			}
		}
	}*/
	Page *pageToWrite = new Page();
	File newFile;
	newFile.Open(0, "mergeFile");
	Record* getRec = new Record();
	int counter = 0;
	int pageIndex = 0;
	if (noMoreRecs == true) {
		while (output->Remove(getRec)) {
			counter++;
			int isPageEmpty = pageToWrite->Append(getRec);
			//cout << isPageEmpty << endl;
			cout << "newfile length -- " << newFile.GetLength() << endl;
			if (isPageEmpty == 0) {

				cout << "newfile length" << newFile.GetLength();
				newFile.AddPage(pageToWrite, pageIndex++);

				pageToWrite->EmptyItOut();

				pageToWrite->Append(getRec);
				
			}
		}
		output->ShutDown();
	}
	

	newFile.AddPage(pageToWrite, pageIndex);
	
	cout << "newfile length" << newFile.GetLength();
	newFile.Close();
	currFile.Close();
	/*while (output->Remove(tempRec)) {
		cout << "Removing records from outPipe \n";
		tempRec->Print(testSchema);
	}*/

	currPage.EmptyItOut();	
}

int SortedFile::GetNew(Record *rec) {

	cout << "getNew" << endl;
	while (toBeMerged->GetFirst(rec)) {
		if (pagePtrForMerge >= currFile.GetLength() - 1)
			return 0;
		else {
			currFile.GetPage(toBeMerged, pagePtrForMerge);
			pagePtrForMerge++;
		}
	}
}

void SortedFile::ChangeReadToWrite(){
	if(m == READ){
		m = WRITE;
		input = new Pipe(PIPE_SIZE);
		output = new Pipe(PIPE_SIZE);
		bq = new BigQ(*input, *output, *si->order, si->runlen);
	}
}

void SortedFile::ChangeWriteToRead(){
	cout << "ChangeWriteToRead \n";
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

int SortedFile::Create(char *f_path, fType f_type, void *startup) {
	
	si = (SortInfo *)startup;

	//Create .bin file. Open if already created

	currFile.Open(0, f_path);
	return 1;
}

void SortedFile::Load(Schema &f_schema, char *loadpath) {

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

	if (si == NULL) {
		si = new SortInfo;
		si->order = new OrderMaker();
	}
	readMetaFile(f_path, si->order, si->runlen);
	cout << "SortedFile::Open \n";
	cout << "runLength is " << si->runlen << endl;
	cout << "OrderMaker is ";
	si->order->Print();

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

	return currFile.Close();
}


	
void SortedFile::Add(Record &rec) {	
	
	//cout << "Adding record to input pipe in SortedFile \n";
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
