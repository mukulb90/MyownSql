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

// Scan returns an iterator to allow the caller to go through the results one by one.
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, // comparision type such as "<" and "="
		const void *value,                    // used in the comparison
		const vector<string> &attributeNames, // a list of projected attributes
		RBFM_ScanIterator &rbfm_ScanIterator) {
	if (!fileHandle.file) {
		return -1;
	}
	rbfm_ScanIterator.fileHandle = fileHandle;
	rbfm_ScanIterator.attributeNames = attributeNames;
	rbfm_ScanIterator.compOp = compOp;
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.value = value;
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


int compare(const void* to, const void* from, const Attribute &attr) {
	if (attr.type == TypeInt) {
		int fromValue = *((int*) from);
		int toValue = *((int*) to);
		return fromValue - toValue;
	} else if (attr.type == TypeReal) {
		float fromValue = *((float*) from);
		float toValue = *((float*) to);
		return fromValue - toValue;
	} else {
		char * fromCursor = (char *) from;
		char * toCursor = (char *) to;
		fromCursor += sizeof(int);
		toCursor += sizeof(int);
		string fromString = string(fromCursor);
		string toString = string(fromCursor);
		return fromString.compare(toString);
	}
}


RBFM_ScanIterator::RBFM_ScanIterator(){

}

RC RBFM_ScanIterator::getNextRecord(RID &returnedRid, void *data) {
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	RID rid;
	void * compAttrValue = malloc(this->conditionAttribute.length);
	if (this->pageNumber == -1 || this->slotNumber == -1) {
//		first call
		this->pageNumber = 0;
		Page* page = fileHandle.file->getPageByIndex(this->pageNumber);
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

	rbfm->readAttribute(fileHandle, recordDescriptor, rid,
						this->conditionAttribute.name, compAttrValue);
				switch (this->compOp) {
				case EQ_OP: {
					if (compare(compAttrValue, this->value,
							this->conditionAttribute) == 0) {
						rbfm->readAttributes(this->fileHandle,
								this->recordDescriptor, rid, this->attributeNames,
								data);
						returnedRid.pageNum = rid.pageNum;
						returnedRid.slotNum = rid.slotNum;
						return 0;
					}
					break;
				}
				case LE_OP: {
					if (compare(compAttrValue, this->value,
							this->conditionAttribute) <= 0) {
						rbfm->readAttributes(this->fileHandle,
								this->recordDescriptor, rid, this->attributeNames,
								data);
						returnedRid.pageNum = rid.pageNum;
						returnedRid.slotNum = rid.slotNum;
						return 0;
					}
					break;
				}
				case LT_OP: {
					if (compare(compAttrValue, this->value,
							this->conditionAttribute) <= 0) {
						rbfm->readAttributes(this->fileHandle,
								this->recordDescriptor, rid, this->attributeNames,
								data);
						returnedRid.pageNum = rid.pageNum;
						returnedRid.slotNum = rid.slotNum;
						return 0;
					}
					break;
				}
				case GT_OP: {
					if (compare(compAttrValue, this->value,
							this->conditionAttribute) > 0) {
						rbfm->readAttributes(this->fileHandle,
								this->recordDescriptor, rid, this->attributeNames,
								data);
						returnedRid.pageNum = rid.pageNum;
						returnedRid.slotNum = rid.slotNum;
						return 0;
					}
					break;
				}
				case GE_OP: {
					if (compare(compAttrValue, this->value,
							this->conditionAttribute) >= 0) {
						rbfm->readAttributes(this->fileHandle,
								this->recordDescriptor, rid, this->attributeNames,
								data);
						returnedRid.pageNum = rid.pageNum;
						returnedRid.slotNum = rid.slotNum;
						return 0;
					}
					break;
				}
				case NE_OP: {
					if (compare(compAttrValue, this->value,
							this->conditionAttribute) != 0) {
						rbfm->readAttributes(this->fileHandle,
								this->recordDescriptor, rid, this->attributeNames,
								data);
						returnedRid.pageNum = rid.pageNum;
						returnedRid.slotNum = rid.slotNum;
						return 0;
					}
					break;
				}
				case NO_OP: {
					rbfm->readAttributes(this->fileHandle, this->recordDescriptor,
							rid, this->attributeNames, data);
					returnedRid.pageNum = rid.pageNum;
					returnedRid.slotNum = rid.slotNum;
					return 0;
				}
			}
	return this->getNextRecord(returnedRid, data);
}


RC RecordBasedFileManager::readAttributes(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const vector<string> &attributeName, void *data){
	return this->readRecord(fileHandle, recordDescriptor, rid, data);
}


