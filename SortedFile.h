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

typedef struct {

	Pipe *pipe;
	Record rec;

}inutil;

typedef struct {

	Pipe *inpipe;
	Pipe *outpipe;
	OrderMaker *order;
}oututil;


class SortedFile : virtual public GenericDBFile {
	int pageNumber;
	int isSpaceEmpty;
	int curPageIndex;
	int pageFlag;
	Page currPage;
	Record *currRec;
	File currFile;
	
	ofstream out;

	OrderMaker* sortorder;
	int runLength;
	mode m;
	BigQ *bq;
	Pipe *input;
	Pipe *output;	

public:
	/*SortedFile();*/
	SortedFile();

	int Create(char *fpath, fType ftype, SortInfo *startup);
	int Open(char *fpath);
	int Close();

	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);

	void ChangeReadToWrite();
	void ChangeWriteToRead();
	void MergeOutputPipeToFile();
	
	// void* consumer(void *arg);
};