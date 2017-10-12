#include <string.h>
#include <math.h>
#include <stdlib.h>

#include "page.h"
#include "internal_record.h"

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
			int length = *cursor;
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
