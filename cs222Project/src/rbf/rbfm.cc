#include <string.h>
#include <thread>
#include <vector>
#include <string.h>

#include "rbfm.h"
#include "math.h"

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
	cout << "Hello World";
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
		Page* page = fileHandle.file->getPageByIndex(i);
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
	if (size < 0){
		return -1;
	}
	char * cursor = (char * ) pageData;
	void * recordData = (void*)malloc(size);
	memcpy(recordData, cursor+offset, size);
	RecordForwarder * recordForwarder = new RecordForwarder();
	recordForwarder->data = recordData;
	recordForwarder->unparse(recordDescriptor, data);
	if (recordForwarder->pageNum == -1) {
		return 0;
	} else {
		RID redirectRID;
		redirectRID.pageNum = recordForwarder->pageNum;
		redirectRID.slotNum = recordForwarder->slotNum;
		RecordBasedFileManager::readRecord(fileHandle, recordDescriptor, redirectRID, data);
	}
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
		*(nullarr+i) = nullstream[k] & (1<<(7 - i%8));
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

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
	if(!fileHandle.file) {
		return -1;
	}
	Page * page = fileHandle.file->pages[rid.pageNum];
	int slotOffset, size;
	int slotNum = rid.slotNum;
	page->getSlot(slotNum, slotOffset, size);
	void * record = malloc(size);
	memcpy(record, (char *)page->data + slotOffset, size);
	InternalRecord* ir = new InternalRecord();
	ir->data = record;
	int index = -1;
	for(int i = 0; i< recordDescriptor.size(); i++) {
		Attribute at = recordDescriptor[i];
		if(at.name == attributeName) {
			index = i;
		}
	}
	ir->getAttributeByIndex(index, recordDescriptor, data);
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	if (!fileHandle.file) {
		return -1;
	}

	void* pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);
	Page page = Page(pageData);
	int delRecOffset, delRecSize, currRecOffset, currRecRize;
	int slotNumber = rid.slotNum;
	int pageSlots = page.getNumberOfSlots();
	page.getSlot(slotNumber, delRecOffset, delRecSize);

	shiftRecords(delRecOffset,delRecSize, page);
	updateSlotDir(delRecOffset, delRecSize, page,  pageSlots);

	delRecSize = -999;
	page.setSlot(slotNumber, delRecOffset, delRecSize);
	page.setFreeSpaceOffset(page.getAvailableSpace() + delRecSize);
	fileHandle.writePage(rid.pageNum, page.data);
	free(pageData);
	return 0;
}

RC RecordBasedFileManager::updateSlotDir(int &currRecordOffset, int &currRecordSize, Page &page, int pageSlots){
	int offset,size;
	for (int i=0; i<pageSlots; i++){
			page.getSlot(i, offset, size);
			if(offset>currRecordOffset && size>0){
				offset = offset - currRecordSize;
				page.setSlot(i,offset,currRecordSize);
			}
		}

	return 0;
}

RC RecordBasedFileManager::shiftRecords(int &currRecordOffset, int &currRecordSize, Page &page){

	int FreeSpaceOffset = page.getFreeSpaceOffset();
	int rightSideDataSize = FreeSpaceOffset - currRecordOffset- currRecordSize -1;
	memcpy((char*)page.data + currRecordOffset, (char*)page.data + currRecordOffset + currRecordSize, rightSideDataSize);
	return 0;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data,
		const RID &rid) {
	// Update Available Space
	if (!fileHandle.file) {
		return -1;
	}
	void* pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);
	Page page = Page(pageData);
	int slotNumber = rid.slotNum;
	int rOffset, rSize;
	int freePageSpace = page.getAvailableSpace();
	int pageSlots = page.getNumberOfSlots();
	RecordForwarder* recordForwarder = RecordForwarder::parse(recordDescriptor,
			data, rid,false);
	int recordSize = recordForwarder->getInternalRecordBytes(recordDescriptor,
			data,false);
	int threshold = freePageSpace - recordSize;
	page.getSlot(slotNumber, rOffset, rSize);

	if (threshold > 0) {
		threshold = recordSize - rSize;
		if (threshold == 0) {
			memcpy((char*) page.data + rOffset, (char*) recordForwarder->data,
					rSize);
			fileHandle.writePage(rid.pageNum, page.data);

		} else {
			RecordBasedFileManager::shiftNUpdateRecord(page, threshold,slotNumber, rOffset, rSize, pageSlots, recordSize,recordForwarder, fileHandle);
			fileHandle.writePage(rid.pageNum, page.data);


		}
	} else {
		RID Updatedrid;
		this->insertRecord(fileHandle, recordDescriptor, data, Updatedrid);
		void *pointerRecord = malloc(12);
		recordForwarder = RecordForwarder::parse(recordDescriptor, pointerRecord, Updatedrid, true);
		int shiftThreshold = FORWARDER_SIZE - rSize;
		int ridSize = 3*sizeof(int);
		int currRecOffset, currRecRize;

		int FreeSpaceOffset = page.getFreeSpaceOffset();
		int rightSideDataSize = FreeSpaceOffset - rOffset- rSize -1;

		memcpy((char*)page.data + rOffset+ rSize + shiftThreshold, (char*)page.data + rOffset + rSize, rightSideDataSize);
		memcpy((char*) page.data + rOffset, (char*) recordForwarder->data,FORWARDER_SIZE);
		///

		for (int i=0; i<pageSlots; i++){
			page.getSlot(i, currRecOffset, currRecRize);
			if(currRecOffset>rOffset && currRecRize>0){
				currRecOffset = currRecOffset +threshold;
				page.setSlot(i,currRecOffset,currRecRize);
				}
		}
		page.setSlot(slotNumber, rOffset, ridSize);
		page.setFreeSpaceOffset(page.getFreeSpaceOffset() - (threshold));
		fileHandle.writePage(rid.pageNum, page.data);
		free(pointerRecord);
	}
	return 0;
}

RC RecordBasedFileManager::shiftNUpdateRecord(Page &page, int threshold,
		int slotNumber, int rOffset, int rSize, int pageSlots, int recordSize,
		 RecordForwarder *recordForwarder, FileHandle &fileHandle) {
	int totalSize = 0;
	int currRecRize, currRecOffset;
	for (int i=0; i<pageSlots; i++){
			page.getSlot(i, currRecOffset, currRecRize);
			if(currRecOffset>rOffset && currRecRize>0){
				currRecOffset = currRecOffset +threshold;
				page.setSlot(i,currRecOffset,currRecRize);
			}
		}

	//shiftRecords(rOffset,rSize,page);
	int FreeSpaceOffset = page.getFreeSpaceOffset();
	int rightSideDataSize = FreeSpaceOffset - rOffset- rSize -1;
	memcpy((char*)page.data + rOffset+ rSize + threshold, (char*)page.data + rOffset + rSize, rightSideDataSize);
	memcpy((char*) page.data + rOffset, (char*) recordForwarder->data,recordSize);

	page.setSlot(slotNumber, rOffset, recordSize);
	page.setFreeSpaceOffset(page.getFreeSpaceOffset() - threshold);

	return 0;
}
