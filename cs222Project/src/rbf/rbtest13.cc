#include <iostream>

#include "pf.h"

using namespace std;

int main(int argc, char **argv) {
	PagedFile * file = new PagedFile("os", 1);
	PagedFile::serialize(file);
	PagedFile* file2 = (PagedFile*)malloc(sizeof(PagedFile));
	PagedFile::deserialize("os", file2);
	cout << file2->name << " : "<< file2->numberOfPages << endl;
	return 0;
}
