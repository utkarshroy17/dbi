#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

struct Util{
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *order;
	int runLen;
	Util(Pipe *i, Pipe *o, OrderMaker *s, int l){
		inPipe = i;
		outPipe = o;
		order = s;
		runLen = l;
	}
	~Util();
};
	
class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
