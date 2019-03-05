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
	virtual int Create(char *fpath, fType ftype, SortInfo *startup) = 0;
	virtual int Open(char *fpath) = 0;
	virtual int Close() = 0;

	virtual void Load(Schema &myschema, const char *loadpath) = 0;

	virtual void MoveFirst() = 0;
	virtual void Add(Record &addme) = 0;
	virtual int GetNext(Record &fetchme) = 0;
	virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;

};


class DBFile {

private:
	GenericDBFile *internalVar;

public:
	DBFile();

	int Create(char *fpath, fType ftype, SortInfo *startup);
	int Open(char *fpath);
	int Close();

	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};
#endif
