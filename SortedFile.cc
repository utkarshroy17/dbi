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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <bits/stdc++.h> 

#define PIPE_SIZE 100

SortedFile::SortedFile() {
	curPageIndex = 0;
	pageFlag = 0;
	pageNumber = 0;
	m = READ;
	readPageBuffer = new Page();
	current = new Record();
	OrderMaker *queryOrder = NULL;
	// input = new Pipe(100);
	// output = new Pipe(100);
}

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
		cout << "set order and runlen" << endl;
		o = sortorder;
		cout << "set order and runlen" << endl;
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

void SortedFile::MergeOutputPipeToFile() {	

	cout << "MergeOutputPipeToFile \n";


	input->ShutDown();

	ComparisonEngine *ce;

	toBeMerged = new Page();
	Record *recFromFile = new Record();
	pagePtrForMerge = 0;
	////GetNew(recFromFile);
	Schema *testSchema = new Schema("catalog", "partsupp");

	Record *tempRec, *tempRecFromFile;
	Record *getRec = new Record();
	Page pageToWrite;
	File newFile;

	newFile.Open(0, "mergeFile");

	int pageIndex = 0;

	//GetNew(recFromFile);
	/*tempRecFromFile = new Record;
	tempRecFromFile->Copy(recFromFile);*/
	
	//recFromFile->Print(testSchema);
	bool noMoreRecs = false;	
	//int result = GetNew(recFromFile);
	/*tempRecFromFile = new Record;
	tempRecFromFile->Copy(recFromFile);*/
	bool leftIsSmaller = false;
	int isPageEmpty = 1;

	//cout << "result = " << result << endl;
	//if (result == 0)
	//	noMoreRecs = true;
	
	int pipeEmpty;
	int fileEmpty;
	Record *pipeRec = new Record();
	//pipeRec = NULL;
	Record *fileRec = new Record();
	//fileRec = NULL;
	bool noPipeRec = true;
	bool noFileRec = true;
	
	while (1) {
		if (noPipeRec) {
			pipeEmpty = !output->Remove(pipeRec);
			if (pipeEmpty)
				break;
			noPipeRec = false;
		}
		if (noFileRec) {
			fileEmpty = !GetNew(fileRec);
			if (fileEmpty)
				break;
			noFileRec = false;
		}
		if (ce->Compare(fileRec, pipeRec, sortorder) < 0) {
			isPageEmpty = pageToWrite.Append(fileRec);
			if (isPageEmpty == 0) {
				newFile.AddPage(&pageToWrite, pageIndex++);
				pageToWrite.EmptyItOut();
				pageToWrite.Append(fileRec);
			}
			noFileRec = true;
		}
		else {
			isPageEmpty = pageToWrite.Append(pipeRec);
			if (isPageEmpty == 0) {
				newFile.AddPage(&pageToWrite, pageIndex++);
				pageToWrite.EmptyItOut();
				pageToWrite.Append(pipeRec);
			}
			noPipeRec = true;
		}
	}
	if (pipeEmpty) {
		cout << "\n\n\n\n\n\nPipe Empty \n";
		if (!noFileRec) {
			isPageEmpty = pageToWrite.Append(fileRec);
			if (isPageEmpty == 0) {
				newFile.AddPage(&pageToWrite, pageIndex++);
				pageToWrite.EmptyItOut();
				pageToWrite.Append(fileRec);
			}
		}
		while (GetNew(fileRec)) {
			isPageEmpty = pageToWrite.Append(fileRec);
			if (isPageEmpty == 0) {
				newFile.AddPage(&pageToWrite, pageIndex++);
				pageToWrite.EmptyItOut();
				pageToWrite.Append(fileRec);
			}
		}
	}
	else if (fileEmpty) {
		cout << "\n\n\n\n\n\nFile Empty \n";
		//skipping one pipeRec
		 do {
			isPageEmpty = pageToWrite.Append(pipeRec);
			if (isPageEmpty == 0) {
				newFile.AddPage(&pageToWrite, pageIndex++);
				pageToWrite.EmptyItOut();
				pageToWrite.Append(pipeRec);
			}
		 } while (output->Remove(pipeRec));
	}

	//while (!noMoreRecs) {	//while there are file recs

	//	if (getRec == NULL)
	//		cout << "get rec is null" << endl;
	//	if(recFromFile == NULL)
	//		cout << "rec from file is null" << endl;

	//	if (output->Remove(getRec)) {	//if there are pipe recs

	//		/*tempRec = new Record;
	//		tempRec->Copy(getRec);*/

	//		while (ce->Compare(recFromFile, getRec, sortorder) < 0) {	//while file rec < pipe rec

	//			/*tempRecFromFile = new Record;
	//			tempRecFromFile->Copy(recFromFile);*/

	//			isPageEmpty = pageToWrite.Append(recFromFile);

	//			if (isPageEmpty == 0) {

	//				newFile.AddPage(&pageToWrite, pageIndex++);

	//				pageToWrite.EmptyItOut();
	//				pageToWrite.Append(recFromFile);
	//			}

	//			if (GetNew(recFromFile) == 0) {

	//				/*tempRecFromFile = new Record;
	//				tempRecFromFile->Copy(recFromFile);*/
	//				noMoreRecs == true;
	//				break;
	//			}

	//			if (getRec == NULL)
	//				cout << "get rec is null in 2nd while" << endl;
	//			if (recFromFile == NULL)
	//				cout << "rec from file is null in 2nd while" << endl;

	//		}

	//		if (pageToWrite.Append(getRec) == 0) {

	//			newFile.AddPage(&pageToWrite, pageIndex++);

	//			pageToWrite.EmptyItOut();
	//			pageToWrite.Append(getRec);
	//		}
	//		
	//	}
	//	else {

	//		do {

	//			/*tempRecFromFile = new Record;
	//			tempRecFromFile->Copy(recFromFile);*/

	//			isPageEmpty = pageToWrite.Append(recFromFile);

	//			if (isPageEmpty == 0) {

	//				newFile.AddPage(&pageToWrite, pageIndex++);

	//				pageToWrite.EmptyItOut();

	//				pageToWrite.Append(recFromFile);
	//			}
	//		} while (GetNew(recFromFile) != 0);
	//		break;
	//	}
	//}

	//
	//if (noMoreRecs == true) {
	//	while (output->Remove(getRec)) {
	//		isPageEmpty = pageToWrite.Append(getRec);
	//		//cout << isPageEmpty << endl;
	//		//if (isPageEmpty == 1)
	//			//cout << "newfile length -- " << newFile.GetLength() << endl;
	//		if (isPageEmpty == 0) {

	//			cout << "newfile length " << newFile.GetLength() << endl;
	//			newFile.AddPage(&pageToWrite, pageIndex++);

	//			pageToWrite.EmptyItOut();

	//			pageToWrite.Append(getRec);
	//			
	//		}
	//	} 
	//	
	//}
	
	cout << "newfile length 1 - " << newFile.GetLength() << endl;

	newFile.AddPage(&pageToWrite, pageIndex);
	
	cout << "newfile length 2 - " << newFile.GetLength() << endl;
	newFile.Close();
	currFile.Close();

	if (rename(fileName, "mergefile.tmp") != 0) {				// making merged file the new file
		cerr << "rename file error!" << endl;
		return;
	}

	remove("mergefile.tmp");

	if (rename("mergeFile", fileName) != 0) {				// making merged file the new file
		cerr << "rename file error!" << endl;
		return;
	}

	readPageBuffer->EmptyItOut();
	currFile.Open(1, fileName);
}

int SortedFile::GetNew(Record *rec) {

	//cout << "getNew " << currFile.GetLength() << " page pointer " << pagePtrForMerge << endl;
	Schema *testSchema = new Schema("catalog", "partsupp");

	while (!this->toBeMerged->GetFirst(rec)) {
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
		bq = new BigQ(*input, *output, *sortorder, runlen);
	}
}

void SortedFile::ChangeWriteToRead(){
	cout << "ChangeWriteToRead \n";
	if(m == WRITE){
		m = READ;
		readPageBuffer->EmptyItOut();
		MergeOutputPipeToFile();
		MoveFirst();
	}
}


int SortedFile::Create(char *f_path, fType f_type, void *startup) {
	
	cout << f_path << endl;
	fileName = new char[strlen(f_path) + 1];
	strcpy(fileName, f_path);

	endOfFile = 1;
	pageIndex = 1;
	//Create .bin file. Open if already created

	currFile.Open(0, f_path);
	return 1;
}

void SortedFile::Load(Schema &f_schema, char *loadpath) {

	// Load the data from the .tbl file to the .bin file

	if (m != WRITE) {
		m = WRITE;
		if (bq == NULL)
			bq = new BigQ(*input, *output, *sortorder, runlen);
	}
	FILE *tableFile = fopen(loadpath, "rb");

	Record temp;

	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {

		input->Insert(&temp);
	}

	fclose(tableFile);
}

int SortedFile::Open(char *f_path) {
	//Open .bin file

	fileName = new char[strlen(f_path) + 1];
	strcpy(fileName, f_path);	

	OrderMaker *o;
	int runlen;
	readMetaFile(f_path, sortorder, runlen);
	cout << "SortedFile::Open \n";
	
	cout << "runLength is " << runlen << endl;
	cout << "OrderMaker is ";
	
	
	sortorder->Print();
	endOfFile = 0;
	pageIndex = 1;

	currFile.Open(1, f_path);
	return 1;
}

void SortedFile::MoveFirst() {

	//Move the pointer to the first record in the file
	pageIndex = 0;

	if (m == READ) {
		// In read mode, so direct movefirst is possible

		if (currFile.GetLength() != 0) {
			currFile.GetPage(readPageBuffer, pageIndex); 

			int result = readPageBuffer->GetFirst(current);		
		}
		else {
			

		}
	}
	else {
		// change mode to read
		m = READ;
		MergeOutputPipeToFile();
		currFile.GetPage(readPageBuffer, pageIndex); 
		readPageBuffer->GetFirst(current);

	}
	queryChange = true;
}

int SortedFile::Close() {

	cout << "\n Calling SortedFile CLOSE \n ";
	ChangeWriteToRead();

	endOfFile = 1;

	return currFile.Close();
}


	
void SortedFile::Add(Record &rec) {	
	
	//cout << "Adding record to input pipe in SortedFile \n";
	ChangeReadToWrite();
	
	input->Insert(&rec);	
}

int SortedFile::GetNext(Record &fetchme) {

	//Get first page
	ChangeWriteToRead();

	if (endOfFile == 1) return 0;

	fetchme.Copy(current);

	if (!readPageBuffer->GetFirst(current)) {

		if (pageIndex >= currFile.GetLength() - 2) {
			endOfFile = 1;
			return 0;
		}
		else {
			pageIndex++;
			currFile.GetPage(readPageBuffer, pageIndex);
			readPageBuffer->GetFirst(current);

		}
	}

	return 1;
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

	ChangeWriteToRead();

	if (queryChange == true) {
		queryOrder = cnf.CreateQueryMaker(*sortorder);
	}

	ComparisonEngine *comp;

	if (queryOrder == NULL) {

		while (GetNext(fetchme)) {


			if (comp->Compare(&fetchme, &literal, &cnf)) {
				return 1;
			}
		}
		return 0;
	}
	else {

		Record *r1 = new Record();

		r1 = GetMatchPage(literal);

		if (r1 == NULL) return 0;

		fetchme.Consume(r1);
		if (comp->Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}

		while (GetNext(fetchme)) {
			if (comp->Compare(&fetchme, &literal, queryOrder) != 0) {
				//not match to query order
				return 0;
			}
			else {
				if (comp->Compare(&fetchme, &literal, &cnf)) {
					//find the right record
					return 1;
				}
			}
		}

	}

	return 0;
}


Record* SortedFile::GetMatchPage(Record &literal) {
	
	if (queryChange) {
		int low = pageIndex;
		int high = currFile.GetLength() - 2;
		int matchPage = binarySearch(low, high, queryOrder, literal);
		
		if (matchPage == -1) {
			
			return NULL;
		}
		if (matchPage != pageIndex) {
			readPageBuffer->EmptyItOut();
			currFile.GetPage(readPageBuffer, matchPage);
			pageIndex = matchPage + 1;
		}
		queryChange = false;
	}

	Record *returnRcd = new Record;
	ComparisonEngine cmp1;
	while (readPageBuffer->GetFirst(returnRcd)) {
		if (cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
			//find the first one
			return returnRcd;
		}
	}
	if (pageIndex >= currFile.GetLength() - 2) {
		return NULL;
	}
	else {
		//since the first record may exist on the next page
		pageIndex++;
		currFile.GetPage(readPageBuffer, pageIndex);
		while (readPageBuffer->GetFirst(returnRcd)) {
			if (cmp1.Compare(returnRcd, &literal, queryOrder) == 0) {
				//find the first one
				return returnRcd;
			}
		}
	}
	return NULL;
}


int SortedFile::binarySearch(int low, int high, OrderMaker *queryOM, Record &literal) {

	cout << "serach OM " << endl;
	queryOM->Print();
	cout << endl << "file om" << endl;
	sortorder->Print();

	if (high < low) return -1;
	if (high == low) return low;
	//high > low

	ComparisonEngine *comp;
	Page *tmpPage = new Page;
	Record *tmpRcd = new Record;
	int mid = (int)(high + low) / 2;
	currFile.GetPage(tmpPage, mid);

	int res;

	Schema nu("catalog", "lineitem");

	tmpPage->GetFirst(tmpRcd);

	tmpRcd->Print(&nu);

	res = comp->Compare(tmpRcd, sortorder, &literal, queryOM);

	//if(res==0){
		//cout<<"FOUND!!!"<<endl;
//		break;
//		}


	delete tmpPage;
	delete tmpRcd;

	//cout<<"compare result"<<res<<"\n";


	if (res == -1) {
		if (low == mid)
			return mid;
		return binarySearch(low, mid - 1, queryOM, literal);
	}
	else if (res == 0) {
		return mid;//binarySearch(low, mid-1, queryOM, literal);
	}
	else
		return binarySearch(mid + 1, high, queryOM, literal);
}

SortedFile::~SortedFile() {
	delete readPageBuffer;
	delete input;
	delete output;
}