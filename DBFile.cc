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
#include "string.h"
#include "fstream"
#include <bits/stdc++.h> 

// stub file .. replace it with your own DBFile.cc

using namespace std;

struct startupStruct{
	OrderMaker *o; 
	int runLength;
};

DBFile::DBFile() {

}

void createMetaFile (char *f_path, fType f_type, void *startup) {	
	char *metaFilename = new char[100];
	sprintf(metaFilename, "%s.header", f_path);
	cout << "Creating Meta File " << metaFilename << endl;
	ofstream meta(metaFilename);
	
	if (f_type == heap){
		meta << "heap\n";
	}else {	//sorted file
		startupStruct *startup1 = static_cast<startupStruct*>(startup);
		string runLength = to_string(startup1->runLength);
		OrderMaker o = *(startup1->o);
		
		int numAtts = o.GetNumAtts();
		int* whichAtts = o.GetWhichAtts();
		Type* whichTypes = o.GetWhichTypes();
		
		meta << "sorted\n";
		meta << runLength << "\n";
		meta << to_string(numAtts) << "\n";
		for(int i = 0; i < numAtts; i++){
			meta << whichAtts[i] << " ";
		}
		meta << endl;
		for(int i = 0; i < numAtts; i++){
			if(whichTypes[i] == Int){
				meta << "Int ";
			}else if(whichTypes[i] == Double){
				meta << "Double ";
			}else {
				meta << "String ";
			}
		}
		meta << endl;
		meta.close();		
	}
}

int DBFile::Create(char *f_path, fType f_type, void *startup) {

	createMetaFile(f_path, f_type, startup);	//TODO: Calling this in Create instead of Close. Does it make a difference?
	//Create .bin file. Open if already created

	ofstream out;
	out.open("metadata.txt");
	
	if (f_type == heap) {
		this->internalVar = new HeapFile();
	}
	else {
		this->internalVar = new SortedFile();
	}

	internalVar->Create(f_path, f_type, startup);
	return 1;
}

int DBFile::Open(char *f_path) {
	//Open .bin file

	char *metaFilename = new char[100];
	sprintf(metaFilename, "%s.header", f_path);
	ifstream infile;
	infile.open(metaFilename);

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

void DBFile::Load(Schema &myschema, char *loadpath) {
	internalVar->Load(myschema, loadpath);
}