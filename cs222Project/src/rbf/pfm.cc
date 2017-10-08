#include "pfm.h"

#include <stdlib.h>
#include <cstdio>
#include <exception>
#include <fstream>
#include <stdio.h>

#include "pf.h"
#include "page.h"
#include "file_util.h"

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
//	free(_pf_manager);
}

RC PagedFileManager::createFile(const string &fileName) {
//	TODO File should be created in a way that it is easy to differentiate these files
	if (fileExists(fileName)) {
		return -1;
	}
	PagedFile* file = new PagedFile(fileName);
	file->serialize(fileName);
	return 0;
}

RC PagedFileManager::destroyFile(const string &fileName) {
	if (!fileExists(fileName)) {
		return -1;
	}
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
	fileHandle.setPagedFile(file);
	return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
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
	fclose(this->serializerHandle);
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
	if(pageNum >= this->file->numberOfPages) {
		return -1;
	}
	int pageId = this->file->pages.at(pageNum);
		Page * page = new Page();
		string pathName = this->file->getPagePathFromPageId(pageId);
		page->deserialize(pathName);
		memcpy(data, page->data, PAGE_SIZE);
		data = page->data;


		this->readPageCounter++;
		string path = FILE_HANDLE_SERIALIZATION_LOCATION;
		this->serialize(path);
		return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
// 	Create a new Page
	if(pageNum >= this->file->numberOfPages) {
		return -1;
	}
	int pageId = this->file->pages.at(pageNum);
	Page * page = new Page((void *)data);
	string pathName = this->file->getPagePathFromPageId(pageId);
	page->serialize(pathName);

//	Increment
	this->writePageCounter++;
	string path = FILE_HANDLE_SERIALIZATION_LOCATION;
	this->serialize(path);
	return 0;
}


RC FileHandle::appendPage(const void *data) {
// 	Create a new Page
	int pageId = this->file->getNextPageId();
	Page * page = new Page((void *)data);
	string pathName = this->file->getPagePathFromPageId(pageId);
	page->serialize(pathName);

//	Add it to the catalog and increment number of pages
	this->file->numberOfPages++;
//	add new page Id to catalog
	this->file->pages.push_back(pageId);
	this->file->serialize(this->file->name);

//	Increment
	this->appendPageCounter++;
	string path = FILE_HANDLE_SERIALIZATION_LOCATION;
	this->serialize(path);
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
