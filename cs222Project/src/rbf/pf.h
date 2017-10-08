#ifndef _pf_h_
#define _pf_h_

#include <string>
#include <sstream>
#include <iostream>
#include "abstract_serializable.h"

using namespace std;

class PagedFile: public Serializable {

private:
	int* pages;

public:
	int numberOfPages;
	string name;

	PagedFile(string fileName);
	PagedFile(string fileName, int numberOfPages);
	~PagedFile();

	int getBytes();
	int getNumberOfPages();
	int mapFromObject(void* data);
	int mapToObject(void* data);
	int getPointerToPageByIndex(int num);
	int getNextPageId();
};

#endif
