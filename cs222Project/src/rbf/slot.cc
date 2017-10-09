#include <string.h>
#include <iostream>
#include "slot.h"

Slot::Slot() {
	cout << "calling slot constructor" <<endl;
	this->offsetToRecord = 0;
	this->recordSize = 0;
}

Slot::Slot(int offsetToRecord, int recordSize) {
	this->offsetToRecord = offsetToRecord;
	this->recordSize = recordSize;
}

int Slot::getBytes() {
	return 2*sizeof(int);
}

int Slot::mapFromObject(void* data) {
	int * cursor = (int*)data;
	memcpy(cursor, &(this->offsetToRecord), sizeof(int));
	memcpy(cursor+1, &(this->recordSize), sizeof(int));
	return 0;
}

int Slot::mapToObject(void* data) {
	int * cursor = (int * ) data;
	memcpy(&(this->offsetToRecord), cursor, sizeof(int));
	memcpy(&(this->recordSize), cursor+1, sizeof(int));
	return 0;
}
