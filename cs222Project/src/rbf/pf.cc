#include "pf.h"
#include "page.h"
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
	memoryOccupied += this->getPageMetaDataSize();
	memoryOccupied += (PAGE_SIZE * this->numberOfPages);
	return memoryOccupied;
}

int PagedFile::mapFromObject(void* data) {
	char * cursor = (char *)data;
	int sizeOfFileName = this->name.length()+1;

	memcpy(cursor, &sizeOfFileName, sizeof(int));
	cursor += sizeof(int);

	memcpy(cursor, this->name.c_str(), sizeOfFileName);
	cursor += sizeOfFileName;

	memcpy(cursor, &(this->numberOfPages), sizeof(int));
	cursor += sizeof(int);

	for(int i=0; i<this->numberOfPages; i++) {
		Page* page = this->pages[i];
		memcpy(cursor, page->data, PAGE_SIZE);
		cursor += PAGE_SIZE;
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

	free(name);
	cursor += sizeOfFileName;

	memcpy(&(this->numberOfPages), cursor, sizeof(int));
	cursor += sizeof(int);

	for(int i=0; i<this->numberOfPages; i++) {
		Page * page = new Page();
		memcpy(page->data, cursor, PAGE_SIZE);
		this->pages.push_back(page);
		cursor += PAGE_SIZE;
	}
	return 0;
}

int PagedFile::getPageStartOffsetByIndex(int num) {
	return this->getPageMetaDataSize() + num*PAGE_SIZE;
}

int PagedFile::getPageMetaDataSize(){
	int memoryOccupied=0;
	memoryOccupied += sizeof(int);
	memoryOccupied += sizeof(char) * this->name.length()+1;
	memoryOccupied += sizeof(int);
	return memoryOccupied;
}
