#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include <fstream>
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum { heap, sorted, tree } fType;

// stub DBFile header..replace it with your own DBFile.h 

struct SortInfo {
	OrderMaker *order;
	int runlen;
};

class GenericDBFile {

public:
	int pageFlag;
	int curPageIndex;
	int isSpaceEmpty;
	Page currPage;
	Record *currRec;
	File currFile;
	/*GenericDbFile();*/
	virtual void Add(Record &addme) = 0;	
	virtual void Load(Schema &myschema, const char *loadpath) = 0;

	void MoveFirst();
	int GetNext(Record &fetchme);
	virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class HeapFile : virtual public GenericDBFile {

public:
	/*HeapFile();*/
	void Add(Record &addme);
	void Load(Schema &myschema, const char *loadpath);

	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class SortedFile : virtual public GenericDBFile {

public:
	/*SortedFile();*/

	void Add(Record &addme);
	void Load(Schema &myschema, const char *loadpath);

	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class DBFile {

private:
	int pageFlag;
	int curPageIndex;
	int isSpaceEmpty;
	fType fileType;
	Page currPage;
	Record *currRec;
	File currFile;
	//GenericDBFile *internalVar;

public:
	DBFile();

	int Create(char *fpath, fType ftype, SortInfo *startup);
	int Open(char *fpath);
	int Close();

	void Add(Record &addme);
	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

	bool fileExists() {
		ifstream ifile("metadata.txt");
		return static_cast<bool>(ifile);
	}
};
#endif
