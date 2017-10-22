#include "pfm.h"
#include <string.h>
#include <stdlib.h>
#include <cstdio>
#include <exception>
#include <fstream>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <iostream>

#include <vector>

using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance() {
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
	delete _pf_manager;
}

RC PagedFileManager::createFile(const string &fileName) {
//	TODO File should be created in a way that it is easy to differentiate these files
	if (fileExists(fileName)) {
		return -1;
	}
	PagedFile* file = new PagedFile(fileName);
	file->serialize(fileName);
	delete file;
	return 0;
}

RC PagedFileManager::destroyFile(const string &fileName) {
	if (!fileExists(fileName)) {
		return -1;
	}
	PagedFile file = PagedFile(fileName);
	file.deserialize(fileName);
	return remove(fileName.c_str());
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	if (!fileExists(fileName)) {
		return -1;
	}
	FILE * handle = fopen(fileName.c_str(), "rb");
	if (!handle) {
		return -1;
	}
	fclose(handle);
	PagedFile* file = new PagedFile(fileName);
	file->deserialize(fileName);
	if(strcmp(file->name.c_str(), fileName.c_str()) != 0) {
		return -1;
	}
	fileHandle.setPagedFile(file);
	return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
	delete fileHandle.file;
	fileHandle.setPagedFile(0);
	return 0;
}

FileHandle::FileHandle() {
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	file = 0;
	serializerHandle = 0;
	deserialerHandle = 0;
	string path = FILE_HANDLE_SERIALIZATION_LOCATION;
	if(fileExists(path)) {
		this->deserialize(path);
	}
}

FileHandle::~FileHandle() {
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
	return this->internalReadPage(pageNum, data, true);
}

RC FileHandle::internalReadPage(PageNum pageNum, void *data, bool shouldAffectCounters) {
	if(pageNum >= this->file->numberOfPages) {
		return -1;
	}
		Page * page = this->file->pages[pageNum];
		memcpy(data, page->data, PAGE_SIZE);

		if(shouldAffectCounters) {
			this->readPageCounter++;
			string path = FILE_HANDLE_SERIALIZATION_LOCATION;
			this->serialize(path);
		}

		return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
// 	Create a new Page
	if(pageNum >= this->file->numberOfPages) {
		return -1;
	}

	Page * page = this->file->pages[pageNum];
//	this->file->serialize(this->file->name);
	memcpy(page->data, data, PAGE_SIZE);
	page->serializeToOffset(this->file->name, this->file->getPageStartOffsetByIndex(pageNum), PAGE_SIZE);

//	#LOCUS
//	delete page;
//	Increment
	this->writePageCounter++;
	string path = FILE_HANDLE_SERIALIZATION_LOCATION;
	this->serialize(path);
	return 0;
}


RC FileHandle::appendPage(const void *data) {
// 	Create a new Page
	Page * newPage = new Page();
	memcpy(newPage->data, data, PAGE_SIZE);

//	Add it to the catalog and increment number of pages
	this->file->numberOfPages++;
//	add new page Id to catalog
	this->file->pages.push_back(newPage);
	this->file->serialize(this->file->name);

//	Increment
	this->appendPageCounter++;
	string path = FILE_HANDLE_SERIALIZATION_LOCATION;
	this->serialize(path);

	//	#LOCUS
//	delete newPage;
	return 0;
}

unsigned FileHandle::getNumberOfPages() {
	return this->file->getNumberOfPages();
}

RC FileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}


void FileHandle::setPagedFile(PagedFile* pagedFile) {
	this->file = pagedFile;
}

int FileHandle::getBytes() {
	return 3*sizeof(unsigned int);
}

int FileHandle::mapFromObject(void* data) {
	unsigned int * cursor = (unsigned int *) data;
	unsigned int * source = &(this->readPageCounter);
	memcpy(cursor, source, sizeof(unsigned int));

	source = &(this->writePageCounter);
	cursor  = cursor + 1;
	memcpy(cursor, source, sizeof(unsigned int));

	source = &(this->appendPageCounter);
	cursor  = cursor+1;
	memcpy(cursor, source, sizeof(unsigned int));

	return 0;
}

int FileHandle::mapToObject(void* data) {
	unsigned int * cursor = (unsigned int *)data;
	memcpy(&(this->readPageCounter), cursor, sizeof(unsigned int));
	cursor += 1;
	memcpy(&(this->writePageCounter), cursor, sizeof(unsigned int));
	cursor += 1;
	memcpy(&(this->appendPageCounter), cursor, sizeof(unsigned int));
	return 0;
}


Serializable::~Serializable() {

}

int Serializable::deserialize(string fileName) {
	return this->deserializeToOffset(fileName, 0, 0);
}

int Serializable::serialize(string fileName) {
	return this->serializeToOffset(fileName, 0, 0);
}


int Serializable::deserializeToOffset(string fileName, int startOffset, int blockSize) {
	int size;
	if(blockSize == 0) {
		size = fsize((char*)fileName.c_str());
	} else {
		size = blockSize;
	}
	void* buffer = malloc(size);
	FILE* handle;
	handle = fopen(fileName.c_str(), "rb") ;
	fread(buffer, size, 1, handle);
	this->mapToObject(buffer);
	free(buffer);
	fclose(handle);
	return 0;
}

int Serializable::serializeToOffset(string fileName,  int startOffset=0, int blockSize=0) {
	int memory;
	if(blockSize == 0) {
		memory = this->getBytes();
	} else {
		memory = blockSize;
	}
	void* buffer = malloc(memory);
	this->mapFromObject(buffer);
	FILE* handle;
	if(startOffset != 0) {
		handle=fopen(fileName.c_str(), "rb+");
		fseek(handle, startOffset, SEEK_SET);
	}
	else {
		handle=fopen(fileName.c_str(), "wb");
	}
	fwrite(buffer, memory, 1, handle);
	free(buffer);
	fclose(handle);
	return 0;
}

bool fileExists(string fileName) {
	FILE* handle = fopen(fileName.c_str(), "rb");
	bool exists = handle != 0;
	if(exists) {
		fclose(handle);
	}
	return exists;
}

unsigned long fsize(char * fileName) {
	if(!fileExists(fileName)) {
		return 0;
	}
	FILE* f = fopen(fileName, "rb");
	fseek(f, 0, SEEK_END);
	unsigned long len = (unsigned long) ftell(f);
	fclose(f);
	return len;
}

Page::Page() {
	this->data = (void *) malloc(PAGE_SIZE);
	this->setFreeSpaceOffset(0);
	this->setNumberOfSlots(0);
}

Page::Page(void * data) {
	this->data = data;
}

int Page::getBytes() {
	return PAGE_SIZE;
}

int Page::mapFromObject(void* data) {
	memcpy(data, this->data, PAGE_SIZE);
	return 0;
}

int Page::mapToObject(void* data) {
	memcpy(this->data, data, PAGE_SIZE);
	return 0;
}

int Page::getFreeSpaceOffset() {
	char * cursor = (char*) this->data;
	int offset = this->getFreeSpaceOffSetPointer();
	int freeSpaceOffset = *((int *)(cursor + offset));
	return freeSpaceOffset;
}

void Page::setFreeSpaceOffset(int value) {
	char * cursor = (char*) this->data;
	int offset = this->getFreeSpaceOffSetPointer();
	cursor += offset;
	int * valu = (int *)cursor;
	memcpy(cursor, &value, sizeof(int));
}

int Page::getFreeSpaceOffSetPointer() {
	return PAGE_SIZE - sizeof(int);
}

int Page::getNumberOfSlots() {
	char * cursor = (char *) data;
	int offset = this->getNumberOfSlotsPointer();
	cursor += offset;
	int numberOfSlots;
	memcpy(&numberOfSlots, cursor, sizeof(int));
	return numberOfSlots;
}

void Page::setNumberOfSlots(int numberOfSlots) {
	char * cursor = (char *) data;
	int offset = this->getNumberOfSlotsPointer();
	cursor += offset;
	memcpy(cursor, &numberOfSlots, sizeof(int));
}

int Page::getNumberOfSlotsPointer() {
	return this->getFreeSpaceOffSetPointer() - 4;
}

int Page::getAvailableSpace() {
	int startOffset = this->getFreeSpaceOffset();
	int spaceOccupiedBySlotDirectory = 0;

//	4 byte for start pointer and number of slots each
	spaceOccupiedBySlotDirectory += 2 * sizeof(int);
	int numberOfSlots = this->getNumberOfSlots();
	for (int i = 0; i < numberOfSlots; i++) {
		spaceOccupiedBySlotDirectory += 2 * sizeof(int);
	}
	return PAGE_SIZE - 1 - startOffset - spaceOccupiedBySlotDirectory;
}

RC Page::insertRecord(const vector<Attribute> &recordDescriptor,
		const void *data, RID &rid) {
//	record size + space for slots
	int recordSize = InternalRecord::getInternalRecordBytes(recordDescriptor, data);
//	int recordSize = this->getRecordSize(recordDescriptor, data);
	InternalRecord* internalRecord = InternalRecord::parse(recordDescriptor, data);
	int spaceRequired = recordSize + 2 * sizeof(int);
	if (this->getAvailableSpace() - spaceRequired >= 0) {
		char* cursor = (char*) this->data;
		int offset  = this->getFreeSpaceOffset();
		memcpy(cursor + offset, internalRecord->data, recordSize);
		this->setNumberOfSlots(this->getNumberOfSlots()+1);
		int currentSlot = this->getNumberOfSlots() - 1;
		this->setSlot(currentSlot, offset, recordSize);
		int newOffset = offset+recordSize;
		this->setFreeSpaceOffset(newOffset);
		newOffset = this->getFreeSpaceOffset();
		rid.slotNum = currentSlot;
		return 0;
	} else {
		return -1;
	}
}

RC Page::setSlot(int &index, int &slot_offset, int &size) {
	char * cursor = (char *) this->data;
	int numberOfSlots = this->getNumberOfSlots();
	if (index < 0 || index > numberOfSlots) {
		return -1;
	}
	int offset = this->getNumberOfSlotsPointer() - (8 * (index+1));
	memcpy(cursor + offset, &slot_offset, sizeof(int));
	memcpy(cursor + offset + sizeof(int), &size, sizeof(int));
	int a, b;
	this->getSlot(index, a, b);
	return 0;
}

RC Page::getSlot(int &index, int &slot_offset, int &size) {
	char * cursor = (char *) this->data;
	int numberOfSlots = this->getNumberOfSlots();
	if (index < 0 || index >= numberOfSlots) {
		return -1;
	}
	int offset = this->getNumberOfSlotsPointer() - (8 * (index+1));
	memcpy(&slot_offset, cursor + offset, sizeof(int));
	memcpy(&size, cursor + offset + sizeof(int), sizeof(int));
	return 0;
}

int Page::getRecordSize(const vector<Attribute> &recordDescriptor,
		const void *data) {
	int size = 0;
	char* cursor = (char*) data;
	int nullBytes = ceil(recordDescriptor.size() / 8.0);

	unsigned char* nullstream  = (unsigned char*)malloc(nullBytes);
	memcpy(nullstream, data, nullBytes);
	bool nullarr[recordDescriptor.size()];

	for(int i=0;  i<recordDescriptor.size(); i++){
		int k = int(i/8);
		nullarr[i] = nullstream[nullBytes-1-k] & (1<<((nullBytes*8)-1-i));
	}
	free(nullstream);
	size += nullBytes;
	cursor += nullBytes;

	for (int i = 0; i < recordDescriptor.size(); i++) {
		Attribute attr = recordDescriptor[i];
		if(nullarr[i]){
			continue;
		}
		if (attr.type == TypeInt || attr.type == TypeReal) {
			size += 4;
			cursor += 4;
		} else {
			int length = *((int *)cursor);
			cursor += 4;
			size += 4;
			cursor += length;
			size += length;
		}

	}
	return size;
}

Page::~Page() {
}


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


InternalRecord::InternalRecord(){
	this->data = 0;
}

int getNumberOfNullBytes(const vector<Attribute> &recordDescriptor){
	return ceil(recordDescriptor.size() / 8.0);
}

bool* getNullBits(const vector<Attribute> &recordDescriptor, const void* data) {
	int nullBytesSize = getNumberOfNullBytes(recordDescriptor);
	unsigned char* nullstream  = (unsigned char*)malloc(nullBytesSize);
		memcpy(nullstream, data, nullBytesSize);
//# TODO free
		bool * nullarr = (bool *)malloc(recordDescriptor.size());

		for(int i=0;  i<recordDescriptor.size(); i++){
			int k = int(i/8);
			*(nullarr+i) = nullstream[nullBytesSize-1-k] & (1<<((nullBytesSize*8)-1-i));
		}
		free(nullstream);
		return nullarr;
}

int InternalRecord::getInternalRecordBytes(const vector<Attribute> &recordDescriptor,const void* data){
	int size = 0;
	char * cursor = (char *)data;
	bool * nullBits = getNullBits(recordDescriptor, data);
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);

	cursor += numberOfNullBytes;
	size += numberOfNullBytes;
	size += sizeof(unsigned short)*(recordDescriptor.size()+1);


	for (int i = 0; i < recordDescriptor.size(); i++) {
			Attribute attr = recordDescriptor[i];
			if(nullBits[i]){
				continue;
			}
			if (attr.type == TypeInt || attr.type == TypeReal) {
				size += 4;
				cursor += 4;
			} else {
				int length = *(int*)cursor;
				cursor += 4;
				cursor += length;
				size += length;
			}

	}

	return size;
}

InternalRecord* InternalRecord::parse(const vector<Attribute> &recordDescriptor,const void* data){
	InternalRecord* record = new InternalRecord();
	bool * nullBits = getNullBits(recordDescriptor, data);
	char * cursor = (char *)data;
	void* internalData = malloc(InternalRecord::getInternalRecordBytes(recordDescriptor, data));
	char * startInternalCursor = (char *)internalData;
	char * internalCursor = (char *)internalData;

	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	memcpy(internalData, cursor, numberOfNullBytes);
	internalCursor += numberOfNullBytes;
	cursor += numberOfNullBytes;

//	Data start from here
	int numberOfAttributes = recordDescriptor.size();
	unsigned short insertionOffset = (unsigned short)numberOfNullBytes + (unsigned short)sizeof(unsigned short)*(numberOfAttributes+1);

	for(int i=0; i<numberOfAttributes; i++){
		Attribute attr = recordDescriptor[i];
		if(*(nullBits+i)){
			memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));
			continue;
		}
		if (attr.type == TypeInt || attr.type == TypeReal) {
			memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));

			memcpy(startInternalCursor + insertionOffset, cursor, 4);
			insertionOffset += (unsigned short)4;
			cursor += 4;
		} else {
		int length = *(int*)cursor;
		cursor += 4;
		memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));
		memcpy(startInternalCursor + insertionOffset, cursor, length);
		cursor += length;
		insertionOffset += length;
		}
	}

	memcpy(internalCursor + numberOfAttributes*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));

	record->data = internalData;
	return record;
}


RC InternalRecord::unParse(const vector<Attribute> &recordDescriptor, void* data) {
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	char * startInternalCursor = (char *) this->data;
	char * internalCursor = (char*)this->data;
	char * cursor = (char *)data;

	vector<bool> nullIndicator;
	vector<int> lengthOfAttributes;

	 memcpy(cursor, internalCursor, numberOfNullBytes);
	 cursor += numberOfNullBytes;
	 internalCursor += numberOfNullBytes;

	for (int i = 0; i < recordDescriptor.size(); i++) {
		unsigned short startOffset = *(unsigned short*)(internalCursor+i*(sizeof(unsigned short)));
		unsigned short endOffSet = *(unsigned short *)(internalCursor+((i+1)*(sizeof(unsigned short))));
		int length = endOffSet - startOffset;
//		cout << i << "-" << length <<endl;
		lengthOfAttributes.push_back(length);
		if(length == 0){
			nullIndicator.push_back(1);
		}
		else {
			nullIndicator.push_back(0);
		}
	}


	for(int i=0; i< recordDescriptor.size(); i++){
		Attribute attr = recordDescriptor[i];
		if(nullIndicator[i]){
			continue;
		}
		unsigned short offsetFromStart =  *(unsigned short*)(internalCursor+i*(sizeof(unsigned short)));
		if (attr.type == TypeInt || attr.type == TypeReal) {
			memcpy(cursor, startInternalCursor + offsetFromStart, 4);
			cursor += 4;
		} else {

		int length = lengthOfAttributes[i];
		memcpy(cursor, &length, 4);
		cursor += 4;

		memcpy(cursor, startInternalCursor + offsetFromStart , length);
		cursor += length;
		}
	}
	return 0;
}

RC InternalRecord::getAttributeByIndex(const int &index, const vector<Attribute> &recordDescriptor, void * attribute) {
	char * cursor = (char *)this->data;
	char * startCursor = (char *)this->data;
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	cursor += numberOfNullBytes;
	unsigned short offsetFromStart = *(unsigned short*)(cursor + sizeof(unsigned short)*index);
	unsigned short nextOffsetFromStart = *(unsigned short*)(cursor + sizeof(unsigned short)*(index+1));
	unsigned short numberOfBytes = nextOffsetFromStart - offsetFromStart;
	memcpy(attribute, startCursor + offsetFromStart, numberOfBytes);
	return 0;
}
