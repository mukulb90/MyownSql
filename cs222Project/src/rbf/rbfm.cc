#include "rbfm.h"

#include <math.h>
#include <stdlib.h>
#include <bitset>
#include <string>
#include <iostream>
#include <unordered_map>
#include <assert.h>

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
	vector<vector<Attribute>> recordDescriptors;
	recordDescriptors.push_back(recordDescriptor);
	return this->internalInsertRecord(fileHandle, recordDescriptors, data, rid, 0);
}

RC RecordBasedFileManager::internalInsertRecord(FileHandle &fileHandle, const vector<vector<Attribute>> &recordDescriptors, const void *data, RID &rid, const int &versionId) {
	vector<Attribute> recordDescriptor = recordDescriptors[versionId];
	if(!fileHandle.file) {
		return -1;
	}
	int numberOfPages = fileHandle.getNumberOfPages();

	for(int i=numberOfPages-1; i>=0 ; i--) {
		Page* page = fileHandle.file->getPageByIndex(i);
		if(page->insertRecord(recordDescriptor, data, rid, versionId) == 0) {
			rid.pageNum = i;
			fileHandle.writePage(i, page->data);
			page->data=0;
			delete page;
			return 0;
		}
	}

	Page* pg = new Page();
	if(pg->insertRecord(recordDescriptor, data, rid, versionId) == 0) {
		fileHandle.appendPage(pg->data);
		rid.pageNum = numberOfPages;
		pg->data=0;
		delete pg;
		return 0;
	}
	pg->data=0;
	delete pg;
	return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	int versionId = 0;
	return this->internalReadRecord(fileHandle, recordDescriptor, rid, data, versionId);
}

RC RecordBasedFileManager::internalReadRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data, int &versionId){
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
	recordForwarder->unparse(recordDescriptor, data, versionId);
	if (recordForwarder->pageNum == -1) {
		page.data=0;
		//freeIfNotNull(recordForwarder->data);
		delete recordForwarder;
		return 0;
	} else {
		RID redirectRID;
		redirectRID.pageNum = recordForwarder->pageNum;
		redirectRID.slotNum = recordForwarder->slotNum;
		RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
		rbfm->internalReadRecord(fileHandle, recordDescriptor, redirectRID, data, versionId);
		page.data=0;
		//freeIfNotNull(recordForwarder->data);
		delete recordForwarder;
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
	vector<vector<Attribute>> recordDescriptors;
	recordDescriptors.push_back(recordDescriptor);
	int versionId = 0;
	return this->internalReadAttribute(fileHandle, recordDescriptors, rid, attributeName, data, versionId);
}

RC RecordBasedFileManager::internalReadAttribute(FileHandle &fileHandle, const vector<vector<Attribute>> &recordDescriptors, const RID &rid, const string &attributeName, void *data, int &versionId) {
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);
	return this->internalReadAttributes(fileHandle, recordDescriptors, rid, attributeNames, data, versionId);
}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::internalScan(FileHandle &fileHandle,
        const vector<vector<Attribute>> &recordDescriptors,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator,
		const int &versionId) {
	if (!fileHandle.file) {
		return -1;
	}
	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.recordDescriptors = recordDescriptors;
	rbfm_ScanIterator.value = value;

	vector<Attribute> recordDescriptor = recordDescriptors[versionId];
	for (int i = 0; i < recordDescriptor.size(); ++i) {
		Attribute attr = recordDescriptor[i];
		if (attr.name == conditionAttribute) {
			rbfm_ScanIterator.conditionAttribute = attr;
			break;
		}
	}
	rbfm_ScanIterator.slotNumber = -1;
	rbfm_ScanIterator.pageNumber = -1;
	return 0;
}

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {
	vector<vector<Attribute>> recordDescriptors;
	recordDescriptors.push_back(recordDescriptor);
	this->internalScan(fileHandle, recordDescriptors, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator, 0);
	return 0;
}


int compare(const void* to, const void* from, const Attribute &attr) {
	char * fromCursor = (char*) from;
	char * toCursor = (char*) to;
//	unsigned char toNullByte = *toCursor;
//	unsigned char fromNullByte = *fromCursor;
//	if(toNullByte == 128 && fromNullByte == 128){
//		return 0;
//	}
//	fromCursor +=1;
	toCursor +=1;

	if (attr.type == TypeInt) {
		int fromValue = *((int*) fromCursor);
		int toValue = *((int*) toCursor);
		return fromValue - toValue;
	} else if (attr.type == TypeReal) {
		float fromValue = *((float*) fromCursor);
		float toValue = *((float*) toCursor);
		return fromValue - toValue;
	} else {
//		int fromLength = *((int*)fromCursor);
		int toLength = *((int*)toCursor);
//		fromCursor += sizeof(int);
		toCursor += sizeof(int);
		string fromString = string(fromCursor);
		string toString = string(toCursor, toLength);
		return fromString.compare(toString);
	}
}


RBFM_ScanIterator::RBFM_ScanIterator(){
	this->value = 0;
	this->compOp = NO_OP;
	this->slotNumber = -1;
	this->pageNumber = -1;
	this->versionId = 0;
}

RBFM_ScanIterator::~RBFM_ScanIterator() {

}

RC RBFM_ScanIterator::getNextRecord(RID &returnedRid, void *data) {
	int rc;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	RID rid;
	if (!fileHandle.file) {
		return -1;
	}
	void * compAttrValue = malloc(this->conditionAttribute.length);
	if (this->pageNumber == -1 || this->slotNumber == -1) {
//		first call
		this->pageNumber = 0;
		Page* page = fileHandle.file->getPageByIndex(this->pageNumber);
		if(page == 0) {
			return -1;
		}
		int numberOfSlots = page->getNumberOfSlots();
		if (numberOfSlots >= 0) {
			this->slotNumber = 0;
		} else {
			return -1;
		}
	} else {
		Page* page = fileHandle.file->getPageByIndex(this->pageNumber);
		int numberOfSlots = page->getNumberOfSlots();
		if(this->slotNumber+1 < numberOfSlots) {
			this->slotNumber++;
		} else {
			this->pageNumber++;
			Page* page = fileHandle.file->getPageByIndex(this->pageNumber);
			if(page == 0) {
				return RBFM_EOF;
			}
			this->slotNumber=0;
		}
	}

	rid.pageNum = this->pageNumber;
	rid.slotNum = this->slotNumber;

	Page* page = fileHandle.file->getPageByIndex(rid.pageNum);
	if(page == 0)
		return -1;

	RecordForwarder *rf = page->getRecord(rid);
	if(rf == 0) {
//		deleted record
		page = 0;
		return this->getNextRecord(returnedRid, data);
	}
	int rfPageNum, rfSlotNum;
	bool dataForwardFlag = rf->isDataForwarder(rfPageNum,rfSlotNum);
	if(dataForwardFlag){
		return this->getNextRecord(returnedRid, data);
	}


	rc = rbfm->internalReadAttribute(fileHandle, recordDescriptors, rid,
						this->conditionAttribute.name, compAttrValue, versionId);

	if(rc != -1) {
		switch (this->compOp) {
						case EQ_OP: {
							if (compare(compAttrValue, this->value,
									this->conditionAttribute) == 0) {
								rbfm->internalReadAttributes(this->fileHandle, this->recordDescriptors, rid, this->attributeNames, data, this->versionId);
								returnedRid.pageNum = rid.pageNum;
								returnedRid.slotNum = rid.slotNum;
								return 0;
							}
							break;
						}
						case LE_OP: {
							if (compare(compAttrValue, this->value,
									this->conditionAttribute) >= 0) {
								rbfm->internalReadAttributes(this->fileHandle, this->recordDescriptors, rid, this->attributeNames, data, this->versionId);
								returnedRid.pageNum = rid.pageNum;
								returnedRid.slotNum = rid.slotNum;
								return 0;
							}
							break;
						}
						case LT_OP: {
							if (compare(compAttrValue, this->value,
									this->conditionAttribute) > 0) {
								rbfm->internalReadAttributes(this->fileHandle, this->recordDescriptors, rid, this->attributeNames, data, this->versionId);
								returnedRid.pageNum = rid.pageNum;
								returnedRid.slotNum = rid.slotNum;
								return 0;
							}
							break;
						}
						case GT_OP: {
							if (compare(compAttrValue, this->value,
									this->conditionAttribute) < 0) {
								rbfm->internalReadAttributes(this->fileHandle, this->recordDescriptors, rid, this->attributeNames, data, this->versionId);
								returnedRid.pageNum = rid.pageNum;
								returnedRid.slotNum = rid.slotNum;
								return 0;
							}
							break;
						}
						case GE_OP: {
							if (compare(compAttrValue, this->value,
									this->conditionAttribute) <= 0) {
								rbfm->internalReadAttributes(this->fileHandle, this->recordDescriptors, rid, this->attributeNames, data, this->versionId);
								returnedRid.pageNum = rid.pageNum;
								returnedRid.slotNum = rid.slotNum;
								return 0;
							}
							break;
						}
						case NE_OP: {
							if (compare(compAttrValue, this->value,
									this->conditionAttribute) != 0) {
								rbfm->internalReadAttributes(this->fileHandle, this->recordDescriptors, rid, this->attributeNames, data, this->versionId);
								returnedRid.pageNum = rid.pageNum;
								returnedRid.slotNum = rid.slotNum;
								return 0;
							}
							break;
						}
						case NO_OP: {
							rbfm->internalReadAttributes(this->fileHandle, this->recordDescriptors, rid, this->attributeNames, data, this->versionId);
							returnedRid.pageNum = rid.pageNum;
							returnedRid.slotNum = rid.slotNum;
							return 0;
						}
					}

	}

	return this->getNextRecord(returnedRid, data);
}

unordered_map<string, Attribute> getAttributeNameToAttributeMap(const vector<Attribute> &attrs){
	unordered_map<string, Attribute> attrsMap;
	for (int i = 0; i < attrs.size(); ++i) {
		Attribute attribute = attrs[i];
		attrsMap[attribute.name] = attribute;
	}
	return attrsMap;
}

void mergeAttributesData(vector<Attribute> newRecordDescriptorForProjections,
	vector<bool> isNullArray, vector<void*> dataArray, void* data){
	char * cursor = (char*) data;
	char * startCursor = (char*) data;

	int numberOfNullBytes = getNumberOfNullBytes(newRecordDescriptorForProjections);
	cursor += numberOfNullBytes;
	bitset<8> byte;
	for (int i = 0; i < newRecordDescriptorForProjections.size(); ++i) {
			Attribute attr = newRecordDescriptorForProjections[i];
			bool isNull = isNullArray[i];
			void * attrData = dataArray[i];
			int bitIndex = 7-(i%8);
			if(isNull) {
				byte.set(bitIndex, 1);
			} else {
				byte.set(bitIndex, 0);
			}
			if(attr.type == TypeInt || attr.type == TypeReal) {
				memcpy(cursor, attrData, sizeof(int));
				cursor += sizeof(int);
			} else {
				int length = *((int*)attrData);
				int attrSize = sizeof(int) + sizeof(char)*length;
				memcpy(cursor, attrData, attrSize);
				cursor += attrSize;
			}

			if(bitIndex == 0 || i == newRecordDescriptorForProjections.size() -1) {
				int byteOffset = i/8;
				unsigned char nullbyte = byte.to_ulong();
				memcpy(startCursor + byteOffset, &nullbyte, 1);
			}
		}

}

RC RecordBasedFileManager::readAttributes(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const vector<string> &attributeNames, void *data){
	vector<vector<Attribute>> recordDescriptors;
	recordDescriptors.push_back(recordDescriptor);
	return this->internalReadAttributes(fileHandle, recordDescriptors, rid, attributeNames, data, 0);
}

RC RecordBasedFileManager::internalReadAttributes(FileHandle &fileHandle, const vector<vector<Attribute>> &recordDescriptors, const RID &rid, const vector<string> &attributeNames, void *data, const int currentVersionId) {
		if(!fileHandle.file) {
				return -1;
		}

		vector<bool> isNullArray;
		vector<void*> dataArray;

		Page * page = fileHandle.file->getPageByIndex(rid.pageNum);
		int slotOffset, size;
		int slotNum = rid.slotNum;
		page->getSlot(slotNum, slotOffset, size);
		if(size < 0) {
			return -1;
		}
		void * record = malloc(size);
		memcpy(record, (char *)page->data + slotOffset, size);
		//RF
		//////////////////////////
		RecordForwarder *rf = new RecordForwarder();
		rf->data = record;
		int rfPageNum, rfSlotNum;
		bool dataForwardFlag = rf->isDataForwarder(rfPageNum,rfSlotNum);
		if(dataForwardFlag){
			RID redirectRid;
			redirectRid.pageNum = rfPageNum;
			redirectRid.slotNum = rfSlotNum;
			return this->internalReadAttributes(fileHandle, recordDescriptors,
					redirectRid, attributeNames, data, currentVersionId);

		}

		InternalRecord* ir = rf->getInternalRecData();
		int recordVersionId;
		ir->getVersionId(recordVersionId);
		vector<Attribute> recordDescriptor = recordDescriptors[recordVersionId];
		unordered_map<string, Attribute> recordDescriptorMap = getAttributeNameToAttributeMap(recordDescriptor);

		vector<Attribute> currentRecordDescriptor = recordDescriptors[currentVersionId];
		unordered_map<string, Attribute> currentRecordDescriptorMap = getAttributeNameToAttributeMap(currentRecordDescriptor);

		vector<Attribute> newRecordDescriptorForProjections;

		for (int i = 0; i < attributeNames.size(); ++i) {
			string attributeName = attributeNames[i];
//			currentRecordDescriptorMap.find(attributeName) == currentRecordDescriptorMap.end()
			if ( currentRecordDescriptorMap.find(attributeName) == currentRecordDescriptorMap.end()) {
				// TODO  show error
				//			  return
			} else {
				Attribute attr = currentRecordDescriptorMap[attributeName];
//				recordDescriptorMap.find(attributeName) == recordDescriptorMap.end()
				if ( recordDescriptorMap.find(attributeName) == recordDescriptorMap.end() ) {
					// new column => add Null Value
					isNullArray.push_back(true);
					int* data = (int*)malloc(sizeof(int));
					*data = 0;
					dataArray.push_back(data);
					continue;
				} else {
					// present on the disk, read from disk and add it to array
					void * attributeData;
					if(attr.type == TypeInt || attr.type == TypeReal) {
						attributeData = malloc(sizeof(int));
					} else {
						attributeData = malloc(PAGE_SIZE);
					}
					int index = -1;
					for(int i = 0; i< recordDescriptor.size(); i++) {
						Attribute at = recordDescriptor[i];
						if(at.name.compare(attributeName) == 0) {
							index = i;
						}
					}
					bool isNull;
					ir->getAttributeByIndex(index, recordDescriptor, attributeData, isNull);
					isNullArray.push_back(isNull);
					dataArray.push_back(attributeData);
				}
				newRecordDescriptorForProjections.push_back(attr);
			}
		}

		mergeAttributesData(newRecordDescriptorForProjections, isNullArray, dataArray, data);

		for (int i = 0; i < attributeNames.size(); ++i) {
//				#TODO free this memory
			//			free(dataArray[i]);
		}
		return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	int rc=0;
	if (!fileHandle.file) {
		return -1;
	}

	void* pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);
	Page page = Page(pageData);
	int delRecOffset, delRecSize, slotNum=rid.slotNum;
	page.getSlot(slotNum, delRecOffset, delRecSize);
	RecordForwarder* rf = page.getRecord(rid);
	int numberOfSlots = page.getNumberOfSlots();
	if(rf == 0) {
		return -1;
	}
	if(delRecSize < 0) {
		return -1;
	}
	int redirectPageNum, redirectSlotNum;
	if(rf->isDataForwarder(redirectPageNum, redirectSlotNum)) {
		RID redirectRID;
		redirectRID.pageNum = redirectPageNum;
		redirectRID.slotNum = redirectSlotNum;
		rc = this->deleteRecord(fileHandle, recordDescriptor, redirectRID);
	}

	if(rc == -1) {
		return -1;
	}

	shiftRecords(delRecOffset,delRecSize, page);
	updateSlotDir(delRecOffset, delRecSize, page,  numberOfSlots);

	int newvariable = page.getFreeSpaceOffset() - delRecSize;
	page.setFreeSpaceOffset(newvariable);
	delRecSize = -999;
	page.setSlot(slotNum, delRecOffset, delRecSize);
	fileHandle.writePage(rid.pageNum, page.data);
	return 0;
}

RC RecordBasedFileManager::updateSlotDir(int &currRecordOffset, int &currRecordSize, Page &page, int pageSlots){
	int offset,size;
	for (int i=0; i<pageSlots; i++){
			page.getSlot(i, offset, size);
			if(offset>currRecordOffset && size>0){
				offset = offset - currRecordSize;
				page.setSlot(i,offset,size);
			}
		}

	return 0;
}

RC RecordBasedFileManager::shiftRecords(int &currRecordOffset, int &currRecordSize, Page &page){

	int FreeSpaceOffset = page.getFreeSpaceOffset();
	int rightSideDataSize = FreeSpaceOffset - currRecordOffset - currRecordSize;

	///
	void* buffer = malloc(4096);
	memcpy((char*) buffer,
			(char*) page.data + currRecordOffset + currRecordSize,
			rightSideDataSize);

	memcpy((char*) page.data + currRecordOffset, (char*) buffer,
			rightSideDataSize);
	free(buffer);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data,
		const RID &rid) {
	vector<vector<Attribute>> recordDescriptors;
	recordDescriptors.push_back(recordDescriptor);
	return this->internalUpdateRecord(fileHandle, recordDescriptors, data, rid, 0);
}


RC RecordBasedFileManager::internalUpdateRecord(FileHandle &fileHandle,
		const vector<vector<Attribute>> &recordDescriptors, const void *data,
		const RID &rid, const int &versionId) {
	// Update Available Space
	if (!fileHandle.file) {
		return -1;
	}
	vector<Attribute> currentRecordDescriptor = recordDescriptors[versionId];
	void* pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);
	Page page = Page(pageData);
	int slotNumber = rid.slotNum;
	int rOffset, rSize;
	int freePageSpace = page.getAvailableSpace();
	int pageSlots = page.getNumberOfSlots();
	RecordForwarder* recordForwarder = RecordForwarder::parse(currentRecordDescriptor,
			data, rid, false, versionId);
	int recordSize = recordForwarder->getInternalRecordBytes(currentRecordDescriptor,
			data, false);
	int threshold = freePageSpace - recordSize;
	int rc = page.getSlot(slotNumber, rOffset, rSize);
	if(rc == -1) {
		return rc;
	}

	if (threshold > 0) {
		int threshold_new = recordSize - rSize;
		if (threshold_new == 0) {
			memcpy((char*) page.data + rOffset, (char*) recordForwarder->data,
					rSize);
			fileHandle.writePage(rid.pageNum, page.data);

		} else {
			RecordBasedFileManager::shiftNUpdateRecord(page, threshold_new,slotNumber, rOffset, rSize, pageSlots, recordSize,recordForwarder, fileHandle);
			fileHandle.writePage(rid.pageNum, page.data);

		}
	} else {
		RID Updatedrid;
		RecordForwarder* rf = page.getRecord(rid);
		int numberOfSlots = page.getNumberOfSlots();
		if (rf == 0) {
			return -1;
		}

		int redirectPageNum, redirectSlotNum;
		if (rf->isDataForwarder(redirectPageNum, redirectSlotNum)) {
			RID redirectRID;
			redirectRID.pageNum = redirectPageNum;
			redirectRID.slotNum = redirectSlotNum;
			rc = deleteRecord(fileHandle, currentRecordDescriptor, redirectRID);
		}
		this->insertRecord(fileHandle, currentRecordDescriptor, data, Updatedrid);
		void *pointerRecord = malloc(12);
		recordForwarder = RecordForwarder::parse(currentRecordDescriptor, pointerRecord, Updatedrid, true, versionId);
		int threshold_new = rSize-FORWARDER_SIZE ;
		int ridSize = 3*sizeof(int);
		int currRecOffset, currRecRize;

		int FreeSpaceOffset = page.getFreeSpaceOffset();
		int rightSideDataSize = FreeSpaceOffset - rOffset- rSize;
		void *buffer = malloc(4096);
		memcpy((char*)buffer, (char*)page.data + rOffset + rSize, rightSideDataSize);
		memcpy((char*)page.data + rOffset+FORWARDER_SIZE, (char*)buffer, rightSideDataSize);

		memcpy((char*) page.data + rOffset, (char*) recordForwarder->data,FORWARDER_SIZE);
		///

		for (int i=0; i<pageSlots; i++){
			page.getSlot(i, currRecOffset, currRecRize);
			if(currRecOffset>rOffset && currRecRize>0){
				currRecOffset = currRecOffset - threshold_new;
				page.setSlot(i,currRecOffset,currRecRize);
				}
		}
		page.setSlot(slotNumber, rOffset, ridSize);
		page.setFreeSpaceOffset(page.getFreeSpaceOffset() - threshold_new);
		fileHandle.writePage(rid.pageNum, page.data);
		free(pointerRecord);
		free(buffer);

	}
	delete recordForwarder;
	return 0;
}

RC RecordBasedFileManager::shiftNUpdateRecord(Page &page, int threshold_new,
		int slotNumber, int rOffset, int rSize, int pageSlots, int recordSize,
		 RecordForwarder *recordForwarder, FileHandle &fileHandle) {
	int currRecRize, currRecOffset;
	for (int i=0; i<pageSlots; i++){
			page.getSlot(i, currRecOffset, currRecRize);
			if(currRecOffset>rOffset && currRecRize>0){
				currRecOffset = currRecOffset + threshold_new;
				page.setSlot(i,currRecOffset,currRecRize);
			}
		}

	//shiftRecords(rOffset,rSize,page);
	int FreeSpaceOffset = page.getFreeSpaceOffset();
	int rightSideDataSize = FreeSpaceOffset - rOffset- rSize;
	void* buffer = malloc(4096);
	memcpy((char*)buffer, (char*)page.data + rOffset + rSize, rightSideDataSize);

	memcpy((char*)page.data + rOffset + rSize + threshold_new, (char*)buffer, rightSideDataSize);
	memcpy((char*) page.data + rOffset, (char*) recordForwarder->data,recordSize);

	page.setSlot(slotNumber, rOffset, recordSize);
	page.setFreeSpaceOffset(page.getFreeSpaceOffset() + threshold_new);
	free(buffer);
	return 0;
}

