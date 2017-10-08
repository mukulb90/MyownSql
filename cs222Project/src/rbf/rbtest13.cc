#include <iostream>

#include "pf.h"
#include <stdio.h>
#include <stdlib.h>

using namespace std;

int main(int argc, char **argv) {
	PagedFile * file = new PagedFile("os");

	file->serialize(file->name);

	PagedFile* file2 = new PagedFile("os");
	file2->deserialize(file->name);
	cout << file2->name << " : "<< file2->getNumberOfPages() << endl;
	return 0;
}
