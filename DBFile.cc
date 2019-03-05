#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include <iostream>
#include <vector>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() {

}

int DBFile::Create(char *f_path, fType f_type, SortInfo *startup) {

	//Create .bin file. Open if already created


	ofstream out;
	out.open("metadata.txt");
	
	if (f_type == heap) {
		out << "heap\n";
		this->internalVar = new HeapFile();
	}
	else {
		out << "sorted\n";
		out << startup->runlen << endl;
		this->internalVar = new SortedFile();
	}

	internalVar->Create(f_path, f_type, startup);
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
		this->internalVar = new SortedFile();
	else
		this->internalVar = new HeapFile();

	internalVar->Open(f_path);
	return 1;
}

int DBFile::Close() {
	//Close an opened File

	return internalVar->Close();
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