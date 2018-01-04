#include "pfm.h"

#include <stdlib.h>

using namespace std;

void freeIfNotNull(void * &data){
	if(data!=0){
		free(data);
		data = 0 ;
	}
}

unordered_map<string, FILE*>* map = new unordered_map<string, FILE*>();

FILE* getFileHandle(const string &fileName,string mode){
    string key = fileName + "-" + mode;
    if(map->find(key) != map->end()) {
//        cout << "Cache Hit" << endl;
        return (*map)[key];
    } else {
//        cout << "Cache MISS" << endl;
        FILE* file = fopen(fileName.c_str(), mode.c_str());
        if(file != NULL) {
            (*map)[key] = file;
        }
        return file;
    }
}

bool operator==(const RID& rid1, const RID& rid2){
	return ((rid1.pageNum == rid2.pageNum) && (rid1.slotNum == rid2.slotNum));
};

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance() {
	if (!_pf_manager)
		_pf_manager = new PagedFileManager();

	return _pf_manager;
}

PagedFileManager::PagedFileManager() {
}

PagedFileManager::~PagedFileManager() {
	_pf_manager = 0;
}

RC PagedFileManager::createFile(const string &fileName) {
//	TODO File should be created in a way that it is easy to differentiate these files
	if (fileExists(fileName)) {
		return -1;
	}
	PagedFile* file = new PagedFile(fileName, NULL);
	file->serialize(fileName);
	delete file;
	return 0;
}

RC PagedFileManager::destroyFile(const string &fileName) {
	if (!fileExists(fileName)) {
		return -1;
	}
	return remove(fileName.c_str());
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	if(fileHandle.file != NULL &&  fileHandle.file->name == fileName) {
		return 0;
	}
	if (!fileExists(fileName)) {
		return -1;
	}
    string mode = "rb+";
	FILE * handle = getFileHandle(fileName, mode);
	if (!handle) {
		return -1;
	}
//	fclose(handle);
	PagedFile* file = new PagedFile(fileName, &fileHandle);
	file->deserialize(fileName);
	if(strcmp(file->name.c_str(), fileName.c_str()) != 0) {
		return -1;
	}
	delete fileHandle.file;
	fileHandle.setPagedFile(file);
	return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
	delete fileHandle.file;
	fileHandle.setPagedFile(NULL);
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
    Page* page = this->file->pagesCache->get(pageNum);
    if(page == 0 || page->id != pageNum) {
    	this->file->missCounter++;
        int rc = this->internalReadPage(pageNum, data, true);
        if(rc == 0) {
            void * copyOfData = malloc(PAGE_SIZE);
            memcpy(copyOfData, data, PAGE_SIZE);
            Page* page = new Page(copyOfData, pageNum);
            this->file->insertPageInCache(page);
        }
        return rc;

    } else {
    	this->file->hitCounter++;
    	this->file->handle->readPageCounter++;
        memcpy(data, page->data, PAGE_SIZE);
        return 0;
    }
}

RC FileHandle::internalReadPage(PageNum pageNum, void *data, bool shouldAffectCounters) {
	if(this->file == 0 || pageNum >= this->file->numberOfPages) {
		return -1;
	}

	Page* page = new Page(data);
	int startOffset = this->file->getPageStartOffsetByIndex(pageNum);
	page->deserializeToOffset(this->file->name, startOffset, PAGE_SIZE);

	if(shouldAffectCounters) {
		this->readPageCounter++;
		string path = FILE_HANDLE_SERIALIZATION_LOCATION;
//		this->serialize(path);
	}
	page->data=0;
	delete page;
	return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
// 	Create a new Page
	if(pageNum >= this->file->numberOfPages) {
		return -1;
	}
    void * copyOfData = malloc(PAGE_SIZE);
    memcpy(copyOfData, data, PAGE_SIZE);
    Page* page = new Page(copyOfData, pageNum);
    page->isDirty = true;
//	page->serializeToOffset(this->file->name, this->file->getPageStartOffsetByIndex(pageNum), PAGE_SIZE);
	this->writePageCounter++;
	string path = FILE_HANDLE_SERIALIZATION_LOCATION;
//	this->serialize(path);
    this->file->insertPageInCache(page);
	return 0;
}


RC FileHandle::appendPage(const void *data) {
    void * copyOfData = malloc(PAGE_SIZE);
    memcpy(copyOfData, data, PAGE_SIZE);
    Page* newPage = new Page(copyOfData, this->file->numberOfPages);
    this->file->insertPageInCache(newPage);
	this->file->numberOfPages++;
	newPage->serializeToOffset(this->file->name,this->file->getPageStartOffsetByIndex(this->file->numberOfPages-1) , PAGE_SIZE);
	this->appendPageCounter++;
	string path = FILE_HANDLE_SERIALIZATION_LOCATION;
//	this->serialize(path);
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


void FileHandle::setPagedFile(PagedFile * pagedFile) {
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
	return this->deserializeToOffset(fileName, -1, 0);
}

int Serializable::serialize(string fileName) {
	return this->serializeToOffset(fileName, -1, 0);
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
    string mode;
	if(startOffset != -1) {
        mode = "rb+";
		handle=getFileHandle(fileName, mode);
		fseek(handle, startOffset, SEEK_SET);
	}
	else {
        mode = "rb+";
		handle=getFileHandle(fileName, mode);
        fseek(handle, 0, SEEK_SET);
    }
	fread(buffer, size, 1, handle);
	this->mapToObject(buffer);
	free(buffer);
//	fclose(handle);
	return 0;
}

int Serializable::serializeToOffset(string fileName,  int startOffset=-1, int blockSize=0) {
	int memory;
	if(blockSize == 0) {
		memory = this->getBytes();
	} else {
		memory = blockSize;
	}
	void* buffer = malloc(memory);
	this->mapFromObject(buffer);
	FILE* handle;
    string mode;
	if(startOffset != -1) {
        mode = "rb+";
		handle=getFileHandle(fileName, mode);
		fseek(handle, startOffset, SEEK_SET);
	}
	else {
        mode  = "wb";
		handle=getFileHandle(fileName, mode);
        fseek(handle, 0, SEEK_SET);
    }
	fwrite(buffer, memory, 1, handle);
	free(buffer);
    fflush(handle);
//	fclose(handle);
	return 0;
}

bool fileExists(string fileName) {
    string mode = "rb+";
	FILE* handle = fopen(fileName.c_str(), mode.c_str());
	bool exists = handle != 0;
	if(exists) {
		fclose(handle);
	}
	return exists;
}

unsigned long fsize(char * fileName) {
	return PAGE_SIZE;
}

Page::Page() {
	this->data = (void *) malloc(PAGE_SIZE);
	this->setFreeSpaceOffset(0);
	this->setNumberOfSlots(0);
	isDirty = false;
}

Page::Page(void * data) {
	this->data = data;
	this->isDirty = false;
}

Page::Page(void* data, int id) {
    this->data = data;
    this->id = id;
}

Page::~Page() {
	freeIfNotNull(this->data);
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

RecordForwarder* Page::getRecord(const RID &rid) {
	int offset, recordSize, slotNum=rid.slotNum;
	int rc = this->getSlot(slotNum, offset, recordSize);
	if(rc == -1 || recordSize< 0) {
		return 0;
	}
	char* cursor = (char*) this->data;
	RecordForwarder* rf = new RecordForwarder();
	rf->data = malloc(PAGE_SIZE);
	memcpy(rf->data, cursor + offset, recordSize);
	return rf;
}

RC Page::insertRecord(const vector<Attribute> &recordDescriptor,
		const void *data, RID &rid, const int &versionId, const int &isPointedByForwarder) {

	RecordForwarder* recordForwarder = RecordForwarder::parse(recordDescriptor, data, rid, false, versionId, isPointedByForwarder);
	int recordSize = recordForwarder->getInternalRecordBytes(recordDescriptor, data, false);
	int pageSlots = this->getNumberOfSlots();
	int RecOffset,RecRize,currentSlot,currSlot,spaceRequired;
	bool deletedSlot = false;

	for (currSlot=0; currSlot<pageSlots; currSlot++){
		this->getSlot(currSlot, RecOffset, RecRize);
		if(RecRize<0){
			deletedSlot = true;
			spaceRequired = recordSize;
			break;
		}
	}

	if(!deletedSlot){
		spaceRequired = recordSize + 2 * sizeof(int);
	}

	if (this->getAvailableSpace() - spaceRequired >= 0) {
		char* cursor = (char*) this->data;
		int offset  = this->getFreeSpaceOffset();
		memcpy(cursor + offset, recordForwarder->data, recordSize);
		if(!deletedSlot){
			this->setNumberOfSlots(this->getNumberOfSlots()+1);
			currentSlot = this->getNumberOfSlots()-1;
		}
		else{
			this->setNumberOfSlots(this->getNumberOfSlots());
			currentSlot = currSlot;
		}
		int newOffset = offset+recordSize;
		this->setSlot(currentSlot, offset, recordSize);
		this->setFreeSpaceOffset(newOffset);
		newOffset = this->getFreeSpaceOffset();
		rid.slotNum = currentSlot;
		delete recordForwarder;
		return 0;
	} else {
		delete recordForwarder;
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
		*(nullarr+i) = nullstream[k] & (1<<(7 - i%8));
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


PagedFile::PagedFile(string fileName, FileHandle *fileHandle) {
	this->name = fileName;
	this->numberOfPages = 0;
	this->handle = 0;
	this->hitCounter = 0;
	this->missCounter = 0;
    this->pagesCache = new Cache<Page*>(PAGES_CACHE_SIZE);
    this->handle = fileHandle;
}


PagedFile::~PagedFile() {
	this->serializeToOffset(this->name, 0, PAGE_SIZE);
	if(this->missCounter+this->hitCounter != 0)
//		cout << "Debug stats: " << this->hitCounter << " - " << this->missCounter << " Ratio: " << this->hitCounter*100/ (this->missCounter+this->hitCounter) << "%" << endl;

	for(int i=0; i< PAGES_CACHE_SIZE; i++) {
		Page* page = this->pagesCache->get(i);
		if(page != 0) {
			if(page->isDirty){
				page->serializeToOffset(this->name, this->getPageStartOffsetByIndex(page->id), PAGE_SIZE);
			}
			delete page;
		}
	}
	delete this->pagesCache;
}

RC PagedFile::insertPageInCache(Page* page) {
	Page* pageAlreadyInCache = this->pagesCache->get(page->id);
	if(pageAlreadyInCache != 0) {
		if(pageAlreadyInCache->id != page->id && pageAlreadyInCache->isDirty) {
			pageAlreadyInCache->serializeToOffset(this->name, this->getPageStartOffsetByIndex(pageAlreadyInCache->id), PAGE_SIZE);
		}
		delete pageAlreadyInCache;
	}
	this->pagesCache->set(page->id, page);
}

int PagedFile::setFileHandle(FileHandle *fileHandle) {
	this->handle = fileHandle;
	return 0;
}

Page* PagedFile::getPageByIndex(int index) {
	if(index >= this->getNumberOfPages()) {
		return 0;
	}
	Page* page = new Page();
	this->handle->readPage(index, page->data);
	this->handle->readPageCounter++;
	return page;
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

	return 0;
}

int PagedFile::mapToObject(void* data) {
	char * cursor = (char *)data;
	int sizeOfFileName;

	memcpy(&sizeOfFileName, cursor, sizeof(int));
	cursor += sizeof(int);

	char * name = (char*)malloc(sizeOfFileName);
	memcpy(name, cursor, sizeOfFileName);
	this->name = string(name);

	free(name);
	cursor += sizeOfFileName;

	memcpy(&(this->numberOfPages), cursor, sizeof(int));
	cursor += sizeof(int);

	return 0;
}

int PagedFile::getPageStartOffsetByIndex(int num) {
	return this->getPageMetaDataSize() + num*PAGE_SIZE;
}

int PagedFile::getPageMetaDataSize(){
	int memoryOccupied=PAGE_SIZE;
	return memoryOccupied;
}


int getNumberOfNullBytes(const vector<Attribute> &recordDescriptor){
	return ceil(recordDescriptor.size() / 8.0);
}

bool* getNullBits(const vector<Attribute> &recordDescriptor, const void* data) {
	int nullBytesSize = getNumberOfNullBytes(recordDescriptor);
	unsigned char* nullstream  = (unsigned char*)malloc(nullBytesSize);
		memcpy(nullstream, data, nullBytesSize);
		//# TODO
		bool * nullarr = (bool *)malloc(recordDescriptor.size());

		for(int i=0;  i<recordDescriptor.size(); i++){
			int k = int(i/8);
			*(nullarr+i) = nullstream[k] & (1<<(7 - i%8));
		}
		free(nullstream);
		return nullarr;
}


InternalRecord::InternalRecord(){
	this->data = 0;
}

InternalRecord::~InternalRecord() {
	freeIfNotNull(this->data);
}

int InternalRecord::getInternalRecordBytes(const vector<Attribute> &recordDescriptor,const void* data){
	int size = 0;
	char * cursor = (char *)data;
	bool * nullBits = getNullBits(recordDescriptor, data);
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);

	size += sizeof(int);
	size += sizeof(int);
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
	freeIfNotNull((void*&)nullBits);
	return size;
}

InternalRecord* InternalRecord::parse(const vector<Attribute> &recordDescriptor,const void* data, const int &versionId, const int &isPointedByForwarder){
	InternalRecord* record = new InternalRecord();
	bool * nullBits = getNullBits(recordDescriptor, data);
	char * cursor = (char *)data;
	void* internalData = malloc(InternalRecord::getInternalRecordBytes(recordDescriptor, data));
	char * startInternalCursor = (char *)internalData;
	char * internalCursor = (char *)internalData;

	memcpy(internalCursor, &versionId, sizeof(int));
	internalCursor += sizeof(int);
	memcpy(internalCursor, &isPointedByForwarder, sizeof(int));
	internalCursor += sizeof(int);
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	memcpy(internalCursor, cursor, numberOfNullBytes);
	internalCursor += numberOfNullBytes;
	cursor += numberOfNullBytes;


//	Data start from here
	int numberOfAttributes = recordDescriptor.size();
	unsigned short insertionOffset = (unsigned short)numberOfNullBytes + (unsigned short)sizeof(unsigned short)*(numberOfAttributes+1) +
			(unsigned short)sizeof(int) + (unsigned short)sizeof(int);

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
	freeIfNotNull((void*&)nullBits);
	return record;
}


RC InternalRecord::unParse(const vector<Attribute> &recordDescriptor, void* data, int &versionId, int &isPointedByForwarder) {
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	char * startInternalCursor = (char *) this->data;
	char * internalCursor = (char*)this->data;
	char * cursor = (char *)data;

	vector<int> lengthOfAttributes;
	 memcpy(&versionId, internalCursor, sizeof(int));
	 internalCursor += sizeof(int);
	 memcpy(&isPointedByForwarder, internalCursor, sizeof(int));
	 internalCursor += sizeof(int);

	 memcpy(cursor, internalCursor, numberOfNullBytes);
	 bool * nullBits = getNullBits(recordDescriptor, cursor);
	 cursor += numberOfNullBytes;
	 internalCursor += numberOfNullBytes;

	for (int i = 0; i < recordDescriptor.size(); i++) {
		unsigned short startOffset = *(unsigned short*)(internalCursor+i*(sizeof(unsigned short)));
		unsigned short endOffSet = *(unsigned short *)(internalCursor+((i+1)*(sizeof(unsigned short))));
		int length = endOffSet - startOffset;
		lengthOfAttributes.push_back(length);
	}


	for(int i=0; i< recordDescriptor.size(); i++){
		Attribute attr = recordDescriptor[i];
		if(*(nullBits + i)){
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
	freeIfNotNull ((void*&)nullBits);
	return 0;
}

RC InternalRecord::getAttributeByIndex(const int &index, const vector<Attribute> &recordDescriptor, void * attribute, bool &isNull) {
	char * attributeCursor = (char * ) attribute;
	Attribute attr = recordDescriptor[index];
	char * cursor = (char *)this->data;
	char * startCursor = (char *)this->data;
	cursor += sizeof(int);
	cursor += sizeof(int);
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	bool* nullBits = getNullBits(recordDescriptor, cursor);
	cursor += numberOfNullBytes;
	unsigned short offsetFromStart = *(unsigned short*)(cursor + sizeof(unsigned short)*index);
	unsigned short nextOffsetFromStart = *(unsigned short*)(cursor + sizeof(unsigned short)*(index+1));
	unsigned short numberOfBytes = nextOffsetFromStart - offsetFromStart;
	if(attr.type == TypeVarChar) {
		int length = (int)numberOfBytes;
		memcpy(attributeCursor, &length, sizeof(int));
		attributeCursor += sizeof(int);
	}
	memcpy(attributeCursor, startCursor + offsetFromStart, numberOfBytes);
	isNull = *(nullBits+index);
	freeIfNotNull((void*&)nullBits);
	return 0;
}

RC InternalRecord::getAttributeValueByName(const string attributeName, const vector<Attribute>& recordDescriptor, void* attribute, bool &isNull) {
	for(int i=0; i< recordDescriptor.size(); i++) {
		Attribute attr = recordDescriptor[i];
		if(attr.name == attributeName) {
			this->getAttributeByIndex(i, recordDescriptor, attribute, isNull);
			return 0;
		}
	}
	return -1;
}

RC InternalRecord::getVersionId(int &versionId) {
	memcpy(&versionId, this->data, sizeof(int));
	return 0;
}

bool InternalRecord::isPointedByForwarder() {
	if(this->data == 0) {
		return false;
	}
	char* cursor = (char*)this->data;
//	skipping version details
	cursor += sizeof(int);
	unsigned isPointed = 0;
	memcpy(&isPointed, cursor, sizeof(unsigned));
	return isPointed == 1;
}

RecordForwarder::RecordForwarder(RID rid) {
	this->pageNum = rid.pageNum;
	this->slotNum = rid.slotNum;
	this->data = 0;

}

RecordForwarder::RecordForwarder(){
	this->pageNum =-1;
	this->slotNum = -1;
	this->data = 0;
}

RecordForwarder::~RecordForwarder() {
	freeIfNotNull(this->data);
}

int RecordForwarder::getInternalRecordBytes(const vector<Attribute> &recordDescriptor,const void * data,bool isForwarderFlag) {
	int bytes = 0;
	bytes += FORWARDER_SIZE;
	if(!isForwarderFlag)
		bytes +=  InternalRecord::getInternalRecordBytes(recordDescriptor,data);
	return bytes;
}

RecordForwarder* RecordForwarder::parse(const vector<Attribute> &recordDescriptor, const void* data, RID rid,bool isForwarderFlag, const int &versionId, const int &isPointedByForwarder) {
	RecordForwarder *recordForwarder = new RecordForwarder(rid);
	int internalDataSize = recordForwarder->getInternalRecordBytes(recordDescriptor, data, isForwarderFlag);
	void * recordForwarderData = malloc(internalDataSize);
	recordForwarder->data = recordForwarderData;
	if(!isForwarderFlag){
		recordForwarder->pageNum = -1;
		recordForwarder->slotNum = -1;
		int forwarder = 0; // Data
		recordForwarder->setForwarderValues(forwarder, recordForwarder->pageNum,recordForwarder->slotNum, rid);
		InternalRecord *internalRecord = InternalRecord::parse(recordDescriptor, data, versionId, isPointedByForwarder);
		memcpy(((char*) recordForwarder->data) + FORWARDER_SIZE,internalRecord->data, internalDataSize - FORWARDER_SIZE);
		delete internalRecord;

	}
	else{
		int forwarder = 1;
		recordForwarder->setForwarderValues(forwarder, recordForwarder->pageNum,recordForwarder->slotNum, rid);

	}
	return recordForwarder;
}

RC RecordForwarder::unparse(const vector<Attribute> &recordDescriptor,void* data, int &versionId, int &isPointedByForwarder) {
	int forwarder = 0;
	int pageNum, slotNum;
	this->getForwarderValues(forwarder, pageNum, slotNum);
	if (forwarder == 0) {
		InternalRecord *internalRecord = new InternalRecord();
		internalRecord->data = ((char*)this->data)+FORWARDER_SIZE;
		internalRecord->unParse(recordDescriptor,data, versionId, isPointedByForwarder);
		internalRecord->data = 0;
		delete internalRecord;
	}
	else {
		this->pageNum = pageNum;
		this->slotNum = slotNum;
	}
	return 0;
}

InternalRecord* RecordForwarder::getInternalRecData() {
	int pageNum, slotNum, forwarder=-1;
	InternalRecord *internalRecord=0;
	this->getForwarderValues(forwarder, pageNum, slotNum);
	if (forwarder == 0) {
		internalRecord = new InternalRecord();
		internalRecord->data = ((char*)this->data)+FORWARDER_SIZE;
	}
	return internalRecord;
}

bool RecordForwarder::isDataForwarder(int &pageNum, int &slotNum){
	int forwarder;
	this->getForwarderValues(forwarder, pageNum, slotNum);
	if(forwarder == 0){
		return false;
	}
	return true;
}

bool RecordForwarder::isPointedByForwarder() {
	InternalRecord* ir = this->getInternalRecData();
	if(ir == 0) {
		return false;
	}
	bool isPointed = ir->isPointedByForwarder();
	ir->data = 0;
	delete ir;
	return isPointed;
}

void RecordForwarder::setForwarderValues(int &forwarder, int &pageNum, int &slotNum, RID rid){
	char * cursor = (char * ) this->data;

	memcpy(cursor, &forwarder, sizeof(int));
	cursor += sizeof(int);

	memcpy(cursor, &pageNum, sizeof(int));
	cursor += sizeof(int);

	memcpy(cursor, &slotNum, sizeof(int));
}

void RecordForwarder::getForwarderValues(int &forwarder,int &pageNum, int &slotNum){
	char * cursor = (char * ) this->data;

	memcpy(&forwarder, cursor, sizeof(int));

	cursor += sizeof(int);

	memcpy( &pageNum, cursor, sizeof(int));
	cursor += sizeof(int);

	memcpy(&slotNum, cursor, sizeof(int));
}

VarcharParser::VarcharParser(){
	this->data = 0;
}

VarcharParser::VarcharParser(void * data){
	this->data = data;
}

VarcharParser::~VarcharParser() {
	freeIfNotNull(this->data);
}

VarcharParser* VarcharParser::parse(const string &str) {
	VarcharParser* varcharParser = new VarcharParser();
	int length = str.size();
	varcharParser->data = malloc(length*sizeof(char)+sizeof(int));
	char * cursor = (char*)varcharParser->data;
	memcpy(cursor, &length, sizeof(int));
	cursor += sizeof(int);
	memcpy(cursor, str.c_str(), length);
	return varcharParser;
}

RC VarcharParser::unParse(string &str){
	char * cursor = (char *)this->data;
	int length;
	memcpy(&length, cursor, sizeof(int));
	cursor += sizeof(int);
	str = string(cursor, length);
	return 0;
};

template<class Value>
Cache<Value>::Cache(int size) {
	this->size = size;
	this->internal_cache = unordered_map<int, Value>();
}

template<class Value>
Cache<Value>::~Cache() {
}

template<class Value>
int Cache<Value>::hashCode(int key){
	return key%(this->size);
}

template<class Value>
Value Cache<Value>::get(int k) {
	int offset = this->hashCode(k);
	if(this->internal_cache.find(offset) != this->internal_cache.end()) {
        return this->internal_cache[offset];
    }
    return 0;
}

template<class Value>
int Cache<Value>::set(int k, Value &v) {
	int offset = this->hashCode(k);
	this->internal_cache[offset] = v;
	return 0;
}


int InternalRecord::getMaxBytes(const vector<Attribute> &recordDescriptor){
		int size = 0;
		size += getNumberOfNullBytes(recordDescriptor);
		Attribute attr;
		for(int i =0; i<recordDescriptor.size(); i++) {
			attr = recordDescriptor[i];
			if(attr.type == TypeReal || attr.type == TypeInt) {
				size += sizeof(int);
			} else {
				size += sizeof(int) + attr.length;
			}
		}
		return size;
}
