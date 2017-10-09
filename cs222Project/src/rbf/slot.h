#ifndef _slot_h_
#define _slot_h_

class Slot {
public:
	void* record;
	int recordSize;

	Slot();
	Slot(void* record, int recordSize);

};

#endif
