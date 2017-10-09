#include<stdlib.h>
#include <iostream>
#include "slot_directory.h"

int SlotDirectory::getNumberOfSlots() {
	return this->slots.size();
}

SlotDirectory::SlotDirectory(){
}

int SlotDirectory::getBytes() {
	int bytes = 0;
	for(int i=0; i<this->getNumberOfSlots(); i++) {
		Slot element = *(this->slots[i]);
		int sizeOfSlot = element.getBytes();
		bytes += sizeOfSlot;
	}
	bytes += sizeof(int);
	return bytes;
}

int SlotDirectory::mapFromObject(void* data) {
	char * cursor = (char *)data;
	int numberOfSlots = this->getNumberOfSlots();
	memcpy(cursor, &numberOfSlots, sizeof(int));
	cursor += sizeof(int);
	for(int i=0; i<numberOfSlots; i++) {
		Slot element = *(this->slots[i]);
		element.mapFromObject(cursor);
		cursor += element.getBytes();
	}
	return 0;
}

int SlotDirectory::mapToObject( void* data) {
	char * cursor = (char *) data;
	int numberOfSlots;
	memcpy(&numberOfSlots, cursor, sizeof(int));
	cursor += sizeof(int);
	for(int i=0; i< numberOfSlots; i++) {
		int * cursor1 = (int * ) cursor;
		Slot * slot = new Slot();
		slot->mapToObject((void*)cursor);
		this->slots.push_back(slot);
		cursor += slot->getBytes();
	}
	return 0;
}
