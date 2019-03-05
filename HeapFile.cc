#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapFile.h"
#include <iostream>


// stub file .. replace it with your own DBFile.cc

HeapFile::HeapFile() {
	curPageIndex = 0;
	pageFlag = 0;
	pageNumber = 0;
}

int HeapFile::Create(char *f_path, fType f_type, SortInfo *startup) {

	//Create .bin file. Open if already created

	currFile.Open(0, f_path);
	return 1;
}

void HeapFile::Load(Schema &f_schema, const char *loadpath) {

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

int HeapFile::Open(char *f_path) {
	//Open .bin file

	currFile.Open(1, f_path);
	return 1;
}

void HeapFile::MoveFirst() {

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

int HeapFile::Close() {
	//Close an opened File

	return currFile.Close();
}

void HeapFile::Add(Record &rec) {

	// Get the last page if a single record is getting added
	if (currFile.GetLength() == 0)
	{
		currPage.Append(&rec);
		currFile.AddPage(&currPage, currFile.GetLength());
		return;
	}

	isSpaceEmpty = currPage.Append(&rec);
	
	if (!isSpaceEmpty)
	{
		currPage.EmptyItOut();
		currPage.Append(&rec);
		currFile.AddPage(&currPage, currFile.GetLength() - 1);
	}
	else
	{
		currFile.AddPage(&currPage, currFile.GetLength() - 2);
	}

}

int HeapFile::GetNext(Record &fetchme) {

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

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {


	ComparisonEngine comp;

	while (GetNext(fetchme)) {
		if (comp.Compare(&fetchme, &literal, &cnf)) {
			return 1;
		}
	}

	return 0;
}
