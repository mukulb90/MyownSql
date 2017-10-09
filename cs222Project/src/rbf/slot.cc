#include <string.h>
#include <iostream>
#include "slot.h"

Slot::Slot() {
	this->record = 0;
	this->recordSize = 0;
}

Slot::Slot(void* record, int recordSize) {
	this->record = record;
	this->recordSize = recordSize;
}
