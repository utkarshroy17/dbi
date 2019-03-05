#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <vector>

// stub file .. replace it with your own DBFile.cc

int pageNumber = 0;

DBFile::DBFile () {
	curPageIndex = 0;
	pageFlag = 0;
	pageNumber = 0;
}

int DBFile::Create (char *f_path, fType f_type, SortInfo *startup) {
	
    //Create .bin file. Open if already created
	
	fileType = f_type;

	if (!fileExists())
	{
		ofstream out;
		out.open("metadata.txt", ios_base::app);
		if (fileType == heap)
			out << "heap\n";
		else {
			out << "sorted\n";
			out << startup->runlen << endl;
		}
	}

	currFile.Open(0, f_path);
	return 1;
}

int DBFile::Open(char *f_path) {
	//Open .bin file

	ifstream infile;

	infile.open("metadata.txt");

	vector<string> lines;
	string temp;

	while (!infile.eof())
	{
		getline(infile, temp);
		lines.push_back(temp);
	}

	infile.close();

	if (lines[0] == "sorted")
		internalVar = new SortedFile();
	else
		internalVar = new HeapFile();

	currFile.Open(1, f_path);
	return 1;
}

int DBFile::Close() {
	//Close an opened File

	return currFile.Close();
}

void DBFile::Add(Record &rec) {
	internalVar->Add(rec);
}

void DBFile::MoveFirst() {
	internalVar->MoveFirst();
}

int DBFile::GetNext(Record &rec) {
	internalVar->GetNext(rec);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	internalVar->GetNext(fetchme, cnf, literal);
}

void DBFile::Load(Schema &myschema, const char *loadpath) {
	internalVar->Load(myschema, loadpath);
}

void GenericDBFile::MoveFirst() {

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

int GenericDBFile::GetNext(Record &fetchme) {

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

void HeapFile::Load (Schema &f_schema, const char *loadpath) {

	// Load the data from the .tbl file to the .bin file
	cout << loadpath << endl;
	FILE *tableFile = fopen(loadpath, "rb");
	
	Record temp;
	
	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {

		Add(temp);
	}

	currFile.AddPage(&currPage, pageNumber);
	pageNumber++;
}

void HeapFile::Add (Record &rec) {

	isSpaceEmpty = currPage.Append(&rec);

	// If page is full, add the page to the file, empty the page, and append the new record
	if (isSpaceEmpty == 0) {
		
		currFile.AddPage(&currPage, pageNumber);
		pageNumber++;

		currPage.EmptyItOut();
		currPage.Append(&rec);
	}
	
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	ComparisonEngine comp;
	
	while (GenericDBFile::GetNext(fetchme)) {
		if (comp.Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}
	}

	return 0;
}


void SortedFile::Load(Schema &f_schema, const char *loadpath) {

	// Load the data from the .tbl file to the .bin file
	cout << loadpath << endl;
	FILE *tableFile = fopen(loadpath, "rb");

	Record temp;

	while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {

		Add(temp);
	}

	currFile.AddPage(&currPage, currFile.GetLength());
}

void SortedFile::Add(Record &rec) {

	cout << "sorted" << endl;

	//isSpaceEmpty = currPage.Append(&rec);

	//// If page is full, add the page to the file, empty the page, and append the new record
	//if (isSpaceEmpty == 0) {

	//	currFile.AddPage(&currPage, currFile.GetLength());

	//	currPage.EmptyItOut();
	//	currPage.Append(&rec);
	//}
	return;

}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

	/*ComparisonEngine comp;

	while (GenericDBFile::GetNext(fetchme)) {
		if (comp.Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}
	}*/

	return 0;
}
