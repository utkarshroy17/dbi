#include <gtest/gtest.h>
#include "test.h"
#include "BigQ.h"
#include <pthread.h>

int err = 0;

void *producer(void *arg) {

	Pipe *myPipe = (Pipe *)arg;

	Record temp;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open(rel->path());
	cout << " producer: opened DBFile " << rel->path() << endl;
	dbfile.MoveFirst();

	while (dbfile.GetNext(temp) == 1) {
		counter += 1;
		//cout << counter << endl;
		//temp.Print(rel->schema());
		if (counter % 100000 == 0) {
			cerr << " producer: " << counter << endl;
		}
		myPipe->Insert(&temp);
	}

	dbfile.Close();
	myPipe->ShutDown();

	//cout << " producer: inserted " << counter << " recs into the pipe\n";
}

void *consumer(void *arg) {

	testutil *t = (testutil *)arg;

	t->order->Print();
	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	if (t->write) {
		sprintf(outfile, "%s.bigq", rel->path());
		dbfile.Create(outfile, heap, NULL);
	}

	//int err = 0;
	int i = 0;

	Record rec[2];

	Record tempRec;
	Record *last = NULL, *prev = NULL;

	/*while (t->pipe->Remove(&tempRec)) {
		last = &tempRec;
		last->Print (rel->schema ());
	}*/

	while (t->pipe->Remove(&rec[i % 2])) {
		prev = last;
		last = &rec[i % 2];

		if (prev && last) {
			if (ceng.Compare(prev, last, t->order) == 1) {
				err++;
			}
			if (t->write) {
				dbfile.Add(*prev);
			}
		}
		if (t->print) {
			cout << "out - ";
			last->Print(rel->schema());
		}
		i++;
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

	if (t->write) {
		if (last) {
			dbfile.Add(*last);
		}
		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
		dbfile.Close();
	}
	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " << err << " recs failed sorted order test \n" << endl;
	}
}


TEST(BigQGoogleTest1, Sort) {

	// sort order for records
	OrderMaker sortorder;
	rel->get_sort_order(sortorder);
	int option = 1;
	int buffsz = 100; // pipe cache size
	Pipe input(buffsz);
	Pipe output(buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	pthread_create(&thread1, NULL, producer, (void *)&input);

	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	testutil tutil = { &output, &sortorder, false, false };
	if (option == 2) {
		tutil.print = true;
	}
	else if (option == 3) {
		tutil.write = true;
	}
	pthread_create(&thread2, NULL, consumer, (void *)&tutil);

	BigQ bq(input, output, sortorder, 10);

	pthread_join(thread1, NULL);
	pthread_join(thread2, NULL);

	EXPECT_EQ(0, err);
}

int main(int argc, char *argv[]) {

	::testing::InitGoogleTest(&argc, argv);
	setup();

	relation *rel_ptr[] = { n, r, c, p, ps, o, li };
	rel = rel_ptr[4];

	cleanup();

	return RUN_ALL_TESTS();
}
