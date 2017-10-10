#ifndef _page_h_
#define _page_h_

#include "abstract_serializable.h"
#include "slot.h"
#include "rbfm.h"

class Page: public Serializable {
public:
	void * data;

	Page();
	Page(void * data);
	~Page();

	int getBytes();
	int mapFromObject(void* data);
	int mapToObject(void* data);
	int getFreeSpaceOffset();
	void setFreeSpaceOffset(int);
	int getFreeSpaceOffSetPointer();
	int getNumberOfSlots();
	void setNumberOfSlots(int);
	int getNumberOfSlotsPointer();
	int getAvailableSpace();
	RC insertRecord(const vector<Attribute> &recordDescriptor, const void *data, RID &rid);
	static int getRecordSize(const vector<Attribute> &recordDescriptor, const void *data);
	RC setSlot(int &index, int &offset, int &size);
	RC getSlot(int &index, int &offset, int &size);
};
#endif
