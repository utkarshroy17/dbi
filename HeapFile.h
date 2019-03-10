#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include <fstream>
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"

class HeapFile : virtual public GenericDBFile {
	int pageNumber;
	int isSpaceEmpty;
	int curPageIndex;
	int pageFlag = 0;
	Page currPage;
	Record *currRec;
	File currFile;

public:
	/*HeapFile();*/

	HeapFile();

	int Create(char *fpath, fType ftype, void *startup);
	int Open(char *fpath);
	int Close();

	void Load(Schema &myschema, char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};