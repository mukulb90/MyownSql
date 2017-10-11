#include "pf.h"
#include <string.h>

using namespace std;

PagedFile::PagedFile(string fileName) {
	this->name = fileName;
	this->numberOfPages = 0;
}


PagedFile::~PagedFile() {
}

int PagedFile::getNumberOfPages() {
	return this->numberOfPages;
}

int PagedFile::getBytes() {
	int memoryOccupied = 0;
	memoryOccupied += sizeof(int);
	memoryOccupied += sizeof(char) * this->name.length();
	memoryOccupied += sizeof(int);
	memoryOccupied += (sizeof(int) * this->numberOfPages);
	return memoryOccupied;
}

int PagedFile::mapFromObject(void* data) {
	char * cursor = (char *)data;
	int sizeOfFileName = this->name.length();

	memcpy(cursor, &sizeOfFileName, sizeof(int));
	cursor += sizeof(int);

	memcpy(cursor, this->name.c_str(), sizeOfFileName);
	cursor += sizeOfFileName;

	memcpy(cursor, &(this->numberOfPages), sizeof(int));
	cursor += sizeof(int);

	for(int i=0; i<this->numberOfPages; i++) {
		memcpy(cursor, &(this->pages[i]), sizeof(int));
		cursor += sizeof(int);
	}
	return 0;
}

int PagedFile::mapToObject(void* data) {
	char * cursor = (char *)data;
	int sizeOfFileName;

	memcpy(&sizeOfFileName, cursor, sizeof(int));
	cursor += sizeof(int);

	char * name = (char*)malloc(sizeof(sizeOfFileName));
	memcpy(name, cursor, sizeOfFileName);
	this->name = string(name);

	//	#LOCUS
	free(name);
	cursor += sizeOfFileName;

	memcpy(&(this->numberOfPages), cursor, sizeof(int));
	cursor += sizeof(int);

	for(int i=0; i<this->numberOfPages; i++) {
		int pageId;
		memcpy(&pageId, cursor, sizeof(int));
		this->pages.push_back(pageId);
		cursor += sizeof(int);
	}
	return 0;
}

int PagedFile::getNextPageId() {
	return this->numberOfPages +1;
}

string PagedFile::getPagePathFromPageId(int pageId) {
	return this->name + "-" + to_string(pageId);
}

void PagedFile::printPages() {
//	 	mght want to do this
	cout << endl;
		for(int i=0; i< this->pages.size(); i++) {
			cout << this->pages.at(i) << "\t";
		}
	cout <<endl;
}
