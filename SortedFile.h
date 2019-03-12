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


class SortedFile : virtual public GenericDBFile {

	char *fileName;
	int pageNumber;
	int isSpaceEmpty;
	int curPageIndex;
	int pageFlag;
	Page currPage;
	Record *currRec;
	File currFile;
	Page *toBeMerged;
	int pagePtrForMerge = 0;
	Page *readPageBuffer;
	int pageIndex;
	Record *current;
	bool queryChange;
	int endOfFile;
	OrderMaker *queryOrder;
	
	ofstream out;

	OrderMaker *sortorder;
	int runlen;

	mode m;
	BigQ *bq;
	Pipe *input;
	Pipe *output;	

public:
	/*SortedFile();*/
	SortedFile();
	~SortedFile();

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
	
	int binarySearch(int low, int high, OrderMaker *queryOM, Record &literal);
	Record* GetMatchPage(Record &literal);
};