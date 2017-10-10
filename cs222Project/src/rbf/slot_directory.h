#ifndef _slot_directory_h_
#define _slot_directory_h_

#include <vector>

#include "abstract_serializable.h"
#include "slot.h"

class SlotDirectory: public Serializable {

public:
	Slot * slots;
	int numberOfSlots;

	SlotDirectory(int numOfSlots);

	int getNumberOfSlots();
	int getBytes();
	int mapFromObject(void* data);
	int mapToObject(void* data);

};

#endif