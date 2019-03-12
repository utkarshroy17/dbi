#include <iostream>
#include <gtest/gtest.h>
#include "DBFile.h"
#include "test.h"

const char *dbfile_dir = ""; // dir where binary heap files should be stored
const char *tpch_dir = "./"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;

relation * rel;

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

TEST(DBFileGoogleTest1, Sort) {

	DBFile dbfile;
	cout << " DBFile will be created at " << rel->path() << endl;
	dbfile.Create(rel->path(), heap, NULL);

	char tbl_path[100]; // construct path of the tpch flat text file
	sprintf(tbl_path, "%s%s.tbl", tpch_dir, rel->name());
	cout << " tpch file will be loaded from " << tbl_path << endl;
	dbfile.Load(*(rel->schema()), tbl_path);
	/*EXPECT_EQ(NULL, );*/
	EXPECT_EQ(1, 1);
	dbfile.Close();

}

TEST(DBFileGoogleTest1, Scan) {

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
	rel = rel_ptr[3];

	return RUN_ALL_TESTS();
}

