#include "pf.h"

#include <iostream>


using namespace std;

PagedFile::PagedFile(string fileName) {
	this->name = fileName;
	this->numberOfPages = 0;
}

PagedFile::PagedFile(string fileName, int numberOfPages) {
	this->name = fileName;
	this->numberOfPages = numberOfPages;
}

int PagedFile::serialize(const PagedFile * pagedFile) {
	FILE * fileHandle = fopen(pagedFile->name.c_str(), "wb");
	if (!fileHandle) {
		return -1;
	}
	int memory = pagedFile->getOccupiedMemory();
	void* buffer = malloc(memory);
	PagedFileObjectMapper::mapFromObject(buffer, pagedFile);
	fwrite (buffer, memory, 1, fileHandle);
	fclose(fileHandle);
	return 0;
}

int PagedFile::deserialize(string fileName, PagedFile * pagedFile) {
	unsigned long size;
	size = fsize((char *)fileName.c_str());
	FILE * fileHandle = fopen(fileName.c_str(), "rb");
	if (!fileHandle) {
		return -1;
	}
	void*  buffer = malloc(size);
	fread(buffer, size, 1, fileHandle);
	PagedFileObjectMapper::mapToObject(pagedFile, buffer);
	return 0;
}

int PagedFile::getOccupiedMemory() const{
		int memoryOccupied = 0;
		memoryOccupied += sizeof(int);
		memoryOccupied += sizeof(int);
		memoryOccupied += sizeof(int);
		memoryOccupied += sizeof(char)*this->name.length() + 1;
		return memoryOccupied;
};

int PagedFileObjectMapper::mapFromObject(void* data, const PagedFile* pagedFile) {

	// typecast it to char pointer to that we can do pointer arithmetic with 1 byte at a time
	char * dataPointer = (char *)data;
	//	store offset pointer to name in first 4 byte
	int offsetPointerToName = 8;
	int sizeOfName = sizeof(char)*(pagedFile->name.length() + 1);
	memcpy(dataPointer, &offsetPointerToName, sizeof(int));
	const char * name  = &(pagedFile->name[0]);
	memcpy(dataPointer+offsetPointerToName, name, sizeOfName);
	int offsetPointerToNumberOfPages = 8 + sizeof(char)*pagedFile->name.length();
	memcpy(dataPointer+4, &offsetPointerToNumberOfPages, sizeof(int));
	memcpy(dataPointer+offsetPointerToNumberOfPages, &(pagedFile->numberOfPages), sizeof(int));
//	store pointer to number of pages in next 4 bytes
	return 0;
}

int PagedFileObjectMapper::mapToObject(PagedFile* pagedFile, const void* data) {
	char * bytePointer = (char *) data;
	// typecast it to char pointer to that we can do pointer arithmetic with 1 byte at a time
	int * pointer = (int *)data;
	int offsetPointerToName = *pointer;
	int offsetPointerToNumberOfPages = *(pointer+1);
	int sizeOfName = offsetPointerToNumberOfPages - offsetPointerToName;
	char * name = (char *) malloc(sizeOfName*sizeof(char));
	memcpy(&(pagedFile->numberOfPages), bytePointer+offsetPointerToNumberOfPages, sizeof(int));
	memcpy(name, bytePointer+offsetPointerToName, sizeOfName);
	pagedFile->name = string(name);
	delete(name);
	return 0;
}
