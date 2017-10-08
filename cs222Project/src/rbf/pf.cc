#include "pf.h"


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
	memoryOccupied += sizeof(int);
	memoryOccupied += sizeof(int);
	memoryOccupied += sizeof(char) * this->name.length() + 1;
	memoryOccupied += sizeof(int) + this->numberOfPages;
	return memoryOccupied;
}

int PagedFile::mapFromObject(void* data) {
//	 typecast it to char pointer to that we can do pointer arithmetic with 1 byte at a time
	char * dataPointer = (char *) data;
	//	store offset pointer to name in first 4 byte
	int offsetPointerToName = 8;
	int sizeOfName = sizeof(char) * (this->name.length() + 1);
	memcpy(dataPointer, &offsetPointerToName, sizeof(int));
	const char * name = &(this->name[0]);
	memcpy(dataPointer + offsetPointerToName, name, sizeOfName);
	int offsetPointerToNumberOfPages = 8 + sizeof(char) * this->name.length();
	memcpy(dataPointer + 4, &offsetPointerToNumberOfPages, sizeof(int));
	memcpy(dataPointer + offsetPointerToNumberOfPages, &(this->numberOfPages),
			sizeof(int));
	//	store pointer to number of pages in next 4 bytes

	int * cursor = (int *)(dataPointer + offsetPointerToNumberOfPages + sizeof(int));
	for(int i=0; i< this->numberOfPages; i++) {

		int *pagename = (int*)malloc(sizeof(int));
		//#   TODO doubt
		pagename =  &this->pages.at(i);
		memcpy(cursor+i, pagename, sizeof(int));
	}
	return 0;
}

int PagedFile::mapToObject(void* data) {
	char * bytePointer = (char *) data;
	// typecast it to char pointer to that we can do pointer arithmetic with 1 byte at a time
	int * pointer = (int *) data;
	int offsetPointerToName = *pointer;
	int offsetPointerToNumberOfPages = *(pointer + 1);
	int sizeOfName = offsetPointerToNumberOfPages - offsetPointerToName;
	char * name = (char *) malloc(sizeOfName * sizeof(char));
	memcpy(&(this->numberOfPages), bytePointer + offsetPointerToNumberOfPages,
			sizeof(int));
	memcpy(name, bytePointer + offsetPointerToName, sizeOfName);
	this->name = string(name);
	int * cursor = (int *)(bytePointer + offsetPointerToNumberOfPages + sizeof(int));
	this->pages = vector<int>();
	for(int i=0; i< this->numberOfPages; i++) {
		int *page = (int *)malloc(sizeof(int));
		memcpy(page, cursor+i, sizeof(int));
		this->pages.push_back(*page);
	}
	return 0;
}

int PagedFile::getNextPageId() {
	return this->numberOfPages +1;
}

string PagedFile::getPagePathFromPageId(int pageId) {
	return this->name + "/" + to_string(pageId);
}
