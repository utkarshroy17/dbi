#include <iostream>
#include <gtest/gtest.h>
#include "DBFile.h"
#include "test.h"

int add_data(FILE *src, int numrecs, int &res) {
	DBFile dbfile;
	dbfile.Open(rel->path());
	Record temp;
	int proc = 0;
	int xx = 20000;

	cout << "number of records in this call - " << numrecs << endl;
	while ((res = temp.SuckNextRecord(rel->schema(), src)) && ++proc < numrecs) {
		//cout << "\n Adding Record in add_data, test.cc  \n";
		// temp.Print(rel->schema());
		dbfile.Add(temp);
		if (proc == xx) cerr << "\t ";
		if (proc % xx == 0) cerr << ".";
	}

	dbfile.Close();
	return proc;
}

TEST(DBFileGoogleTest1, SortSmall) {

	OrderMaker o;
	rel->get_sort_order(o);

	int runlen = 3;
	//while (runlen < 1) {
	///*	cout << "\t\n specify runlength:\n\t ";
	//	cin >> runlen;*/
	//}

	struct { OrderMaker *o; int l; } startup = { &o, runlen };

	DBFile dbfile;
	cout << "\n output to dbfile : " << rel->path() << endl;
	dbfile.Create(rel->path(), sorted, &startup);
	dbfile.Close();

	char tbl_path[100];
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	cout << " input from file : " << tbl_path << endl;

	FILE *tblfile = fopen(tbl_path, "r");

	srand48(time(NULL));

	int x = 2;
	int proc = 1, res = 1, tot = 0;
	while (proc && res) {

		proc = add_data(tblfile, lrand48() % (int)pow(1e3, x) + (x - 1) * 1000, res);
		tot += proc;
		if (proc)
			cout << "\n\t added " << proc << " recs..so far " << tot << endl;

	}
	cout << "\n create finished.. " << tot << " recs inserted\n";
	fclose(tblfile);
	EXPECT_EQ(8000, tot);

}

TEST(DBFileGoogleTest2, SortBig) {

	OrderMaker o;
	rel->get_sort_order(o);

	int runlen = 3;
	//while (runlen < 1) {
	///*	cout << "\t\n specify runlength:\n\t ";
	//	cin >> runlen;*/
	//}

	struct { OrderMaker *o; int l; } startup = { &o, runlen };

	DBFile dbfile;
	cout << "\n output to dbfile : " << rel->path() << endl;
	dbfile.Create(rel->path(), sorted, &startup);
	dbfile.Close();

	char tbl_path[100];
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	cout << " input from file : " << tbl_path << endl;

	FILE *tblfile = fopen(tbl_path, "r");

	srand48(time(NULL));

	int x = 1;
	int proc = 1, res = 1, tot = 0;
	while (proc && res) {

		proc = add_data(tblfile, lrand48() % (int)pow(1e3, x) + (x - 1) * 1000, res);
		tot += proc;
		if (proc)
			cout << "\n\t added " << proc << " recs..so far " << tot << endl;

	}
	cout << "\n create finished.. " << tot << " recs inserted\n";
	fclose(tblfile);
	EXPECT_EQ(8000, tot);

}


TEST(DBFileGoogleTest3, Scan) {

	DBFile dbfile;
	dbfile.Open(rel->path());
	dbfile.MoveFirst();

	Record temp;

	int counter = 0;
	while (dbfile.GetNext(temp) == 1) {
		counter += 1;
		temp.Print(rel->schema());
		if (counter % 10000 == 0) {
			cout << counter << "\n";
		}
	}

	EXPECT_EQ(2002, counter);
	dbfile.Close();
}


int main(int argc, char** argv) {

	::testing::InitGoogleTest(&argc, argv);
	setup(catalog_path, dbfile_dir, tpch_dir);

	relation *rel_ptr[] = { n, r, c, p, ps, o, li };
	rel = rel_ptr[4];

	return RUN_ALL_TESTS();
}

