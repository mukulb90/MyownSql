#ifndef _pf_h_
#define _pf_h_

#include "page.h"
#include <string>
#include <sstream>
#include <iostream>
#include "abstract_serializable.h"
#include <vector>

using namespace std;

class PagedFile: public Serializable {

public:
	string name;
	int numberOfPages;
	vector<Page *> pages;

	PagedFile(string fileName);
	~PagedFile();

	int getBytes();
	int getNumberOfPages();
	int mapFromObject(void* data);
	int mapToObject(void* data);
	int getPageStartOffsetByIndex(int num);
	int getPageMetaDataSize();
};

#endif
