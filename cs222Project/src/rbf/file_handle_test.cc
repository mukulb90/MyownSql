#include <cassert>
#include <stdio.h>
#include "pf.h"

int main(int argc, char **argv) {

	PagedFile * file = new PagedFile("test_pf_serialization");
	file->pages.push_back(++file->numberOfPages);
	cout << file->pages.at(0) << endl;
	file->serialize(file->name);

	PagedFile * file2 = new PagedFile("test_pf_serialization");
	file2->deserialize(file2->name);

	assert(file2->name == file->name);
	assert(file2->numberOfPages == file->numberOfPages);
	assert(file2->pages[0] == file->pages[0]);
	cout << "All Test Cases Passed";
	return 0;

}

