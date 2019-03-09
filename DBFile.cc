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
// #include "iostream"
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

int readMetaFile (char *f_path) {
	string meta_name(f_path);
	meta_name.replace(strlen(f_path) - 4, 4, "-meta");
	cout << "Opening Meta File " << meta_name << endl;
	string line;
	ifstream meta (meta_name);
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

		OrderMaker dummy;
		dummy.Set(numAtts, whichAtts, whichTypes);
		dummy.Print();
		
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

void createMetaFile (char *f_path, fType f_type, void *startup) {
	string meta_name(f_path);
	meta_name.replace(strlen(f_path) - 4, 4, "-meta");
	cout << "Creating Meta File " << meta_name << endl;
	
	ofstream meta(meta_name);
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

int DBFile::Create(char *f_path, fType f_type, SortInfo *startup) {

	//TODO: Place in right path
	createMetaFile(f_path, f_type, startup);
	readMetaFile(f_path);
	
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