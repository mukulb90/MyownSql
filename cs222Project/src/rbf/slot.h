#ifndef _slot_h_
#define _slot_h_

#include "abstract_serializable.h"

class Slot: public Serializable {
public:

	int offsetToRecord;
	int recordSize;

	Slot();
	Slot(int offsetToRecord, int recordSize);

	int getBytes();
	int mapFromObject(void* data);
	int mapToObject(void* data);

};

#endif
