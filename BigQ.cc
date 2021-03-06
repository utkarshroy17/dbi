#include "BigQ.h"
#include <bits/stdc++.h>
#include <vector>

ComparisonEngine ceng;
OrderMaker *g_order;
File tempFile;
int PageNumber = 0;
vector<Record*> recVector;

//TODO : Remove region and catalog path
// void mergeRuns( threadutil *tu) {
void mergeRuns( Util *tu) {

	vector<Page*> pageVector;
	vector<Record*> recordVector;
	File file;
	Page* tempPage;
	Record *printRec;
	int min = 0;
	
	file.Open(1, "temp");

	int n = file.GetLength();

	for (int i = 0; i < n-1; i++) {
		tempPage = new Page;
		file.GetPage(tempPage, i);
		pageVector.push_back(tempPage);
	}

	int m = pageVector.size();

	for (int i = 0; i < m; i++)
		recordVector.push_back(NULL);
		
	while (pageVector.size() > 0) {
				
		for (int j = 0; j < recordVector.size(); j++) {

			Record *tempRec = new Record;
			if (recordVector[j] == NULL) {
				
				if (pageVector[j]->GetFirst(tempRec) == 0) {
					pageVector.erase(pageVector.begin() + j);
					recordVector.erase(recordVector.begin() + j);
				}		
				else {
					recordVector[j] = tempRec;
					//recordVector[j]->Print(testSchema);
				}				
			}
		}				

		min = 0;
		for (int j = 1; j < recordVector.size(); j++) {
			if (ceng.Compare(recordVector[min], recordVector[j], g_order) > 0)
				min = j;
		}
		if (recordVector.size() > 0) {
			tu->outPipe->Insert(recordVector[min]);

			recordVector[min] = NULL;
			min = 0;
		}
		
	}
	
	file.Close();

}

bool compRecs(Record *left, Record *right)
{
	int compVal = ceng.Compare(left, right, g_order);

	if (compVal < 0)
		return true;
	else
		return false;
}

// void addRunToFile(threadutil *tu) {
void addRunToFile(Util *tu) {

	Record *curRec = NULL;
	Page curPage;
	int isPageEmpty;

	for (int j = 0; j < recVector.size(); j++) {

		curRec = recVector[j];

		isPageEmpty = curPage.Append(curRec);

		if (!isPageEmpty) {

			tempFile.AddPage(&curPage, PageNumber);
			curPage.EmptyItOut();
			curPage.Append(curRec);

			PageNumber++;
		}
	}

	tempFile.AddPage(&curPage, PageNumber);
	PageNumber++;
}

void *sortRecs(void *arg) {
	
	// threadutil *tu = (threadutil*)arg;
	Util* tu = (Util*)arg;

	cout << "Calling sortRecs, BigQ.cc \n";
	tu->inPipe->Get();
	
	int numRuns = 0, isPageEmpty, numPages = 0;
	Record *tempRecord, *recInsert;
	Record *getRec = new Record();
	Page curPage, tempPage;
	
	tempFile.Open(0, "temp");
	
	
	char *region = "partsupp";
	char *catalog_path = "catalog";
	Schema *testSchema = new Schema(catalog_path, region);	//TODO: this is hardcoded to religion. Change it

	while (tu->inPipe->Remove(getRec)) {
	
	getRec->Print(testSchema);
		
		
		cout << "Called tu->inPipe->Remove sortRecs, BiqQ.cc \n";

		tempRecord = new Record;
		tempRecord->Copy(getRec);

		recVector.push_back(tempRecord);

		recInsert = new Record;
		recInsert->Copy(recVector.back());
		isPageEmpty = tempPage.Append(recInsert);

		if (isPageEmpty == 0) {

			tempPage.EmptyItOut();
			tempPage.Append(recInsert);
			numPages++;

			if (numPages == tu->runLen) {
				
				sort(recVector.begin(), recVector.end(), compRecs);
				addRunToFile(tu);
				numPages = 0;
				recVector.clear();
			}
		}
	}

	if (recVector.size() > 0) {

		sort(recVector.begin(), recVector.end(), compRecs);
		addRunToFile(tu);
		recVector.clear();
		numPages = 0;
		numRuns++;
	}

	tempFile.Close();

	mergeRuns(tu);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	cout << "Calling BigQ constructor \n";

	// threadutil tu = { &in, &out, &sortorder, runlen };
	Util* util = new Util(&in, &out, &sortorder, runlen);
	g_order = &sortorder;

	pthread_t worker;

	// pthread_create(&worker, NULL, sortRecs, (void *)&tu);
	pthread_create(&worker, NULL, sortRecs, (void *)util);

	cout << "Worker Thread Created BigQ.cc \n";
	// pthread_join(worker, NULL); //TODO: When to call BigQ worker thread join?
	// cout << "Worker Thread joined BigQ.cc \n";

  // construct priority queue over sorted runs and dump sorted data into the out pipe
  // finally shut down the out pipe
	
	// out.ShutDown();
	
}

BigQ::~BigQ () {
}
