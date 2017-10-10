#ifndef _pf_h_
#define _pf_h_

#include <string>
#include <sstream>
#include <iostream>
#include "abstract_serializable.h"
#include <vector>

using namespace std;

class PagedFile: public Serializable {

public:
	int numberOfPages;
	string name;
	vector<int> pages;

	PagedFile(string fileName);
	PagedFile(string fileName, int numberOfPages);
	~PagedFile();

	int getBytes();
	int getNumberOfPages();
	int mapFromObject(void* data);
	int mapToObject(void* data);
	int getPointerToPageByIndex(int num);
	int getNextPageId();
	string getPagePathFromPageId(int pageId);
	void printPages();
};

#endif
