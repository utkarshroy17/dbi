#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include <fstream>
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Pipe.h"
#include <vector>
#include "BigQ.h"

typedef enum { READ, WRITE } mode;

//typedef struct {
//
//	Pipe *pipe;
//	Record rec;
//
//}inutil;
//
//typedef struct {
//
//	Pipe *inpipe;
//	Pipe *outpipe;
//	OrderMaker *order;
//}oututil;

struct SortInfo {
	OrderMaker *order;
	int runlen;
};


class SortedFile : virtual public GenericDBFile {
	int pageNumber;
	int isSpaceEmpty;
	int curPageIndex;
	int pageFlag;
	Page currPage;
	Record *currRec;
	File currFile;
	Page *toBeMerged;
	int pagePtrForMerge = 0;
	SortInfo *si;
	
	ofstream out;

	mode m;
	BigQ *bq;
	Pipe *input;
	Pipe *output;	

public:
	/*SortedFile();*/
	SortedFile();

	int Create(char *fpath, fType ftype, void *startup);
	int Open(char *fpath);
	int Close();

	void Load(Schema &myschema, char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

	void ChangeReadToWrite();
	void ChangeWriteToRead();
	void MergeOutputPipeToFile();
	int GetNew(Record *rec);
	
	// void* consumer(void *arg);
};