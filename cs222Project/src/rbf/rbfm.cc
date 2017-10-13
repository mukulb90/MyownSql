#include <string.h>
#include <thread>
#include <vector>
#include <string.h>

#include "rbfm.h"
#include "page.h"
#include "math.h"
#include "internal_record.h"

using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	delete _rbf_manager;
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	PagedFileManager* pfm = PagedFileManager::instance();
	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	PagedFileManager* pfm = PagedFileManager::instance();
	return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	PagedFileManager* pfm = PagedFileManager::instance();
	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	PagedFileManager* pfm = PagedFileManager::instance();
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	if(!fileHandle.file) {
		return -1;
	}
	int numberOfPages = fileHandle.getNumberOfPages();

	for(int i=numberOfPages-1; i>=0 ; i--) {
		Page* page = fileHandle.file->pages[i];
		if(page->insertRecord(recordDescriptor, data, rid) == 0) {
			rid.pageNum = i;
			fileHandle.writePage(i, page->data);
			return 0;
		}
	}

	Page* pg = new Page();
	if(pg->insertRecord(recordDescriptor, data, rid) == 0) {
		fileHandle.appendPage(pg->data);
		rid.pageNum = numberOfPages;
		free(pg->data);
		delete pg;
		return 0;
	}
	free(pg->data);
	delete pg;
	return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	if(!fileHandle.file) {
			return -1;
	}
	void* pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);
	Page page = Page(pageData);
	int offset, size;
	int slotNumber = rid.slotNum;
	page.getSlot(slotNumber, offset, size);
	char * cursor = (char * ) pageData;
	void * internalData = (void*)malloc(size);
	memcpy(internalData, cursor+offset, size);
	InternalRecord * internalRecord = new InternalRecord();
	internalRecord->data = internalData;
	internalRecord->unParse(recordDescriptor, data);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int nullBytes = ceil(recordDescriptor.size() / 8.0);
	char * cursor = (char *) data;

	unsigned char* nullstream  = (unsigned char*)malloc(nullBytes);
	memcpy(nullstream,data,nullBytes);
	bool nullarr[recordDescriptor.size()];

	for(int i=0;  i<recordDescriptor.size(); i++){
		int k = int(i/8);
		nullarr[i] = nullstream[nullBytes-1-k] & (1<<((nullBytes*8)-1-i));
	}
	free(nullstream);

	cursor += nullBytes;
	cout << "Record:-";
	for(int i=0; i<recordDescriptor.size(); i++) {
		Attribute attr = recordDescriptor[i];
		if(nullarr[i]) {
			cout  << attr.name << ":" << "NULL" << "\t";
			continue;
		}
		if (attr.type == TypeInt ) {
			int * int_cursor = (int *)cursor;
			cursor += sizeof(int);
			cout  << attr.name << ":" << *int_cursor << "\t";
		} else if(attr.type == TypeReal) {
			float * float_cursor = (float *)cursor;
			cursor += sizeof(float);
			cout  << attr.name << ":" << *float_cursor << "\t";
		} else {
			int length = *((int *)cursor);
			cursor += sizeof(int);
			cout  << attr.name << ":";
			for(int j=0; j<length; j++) {
				cout << *cursor;
				cursor += 1;
			}
			cout << "\t";
		}
	}
	cout << endl;
    return 0;
}
