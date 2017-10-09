#include <iostream>
#include "slot_directory.h"
#include <cassert>
using namespace std;

int main(int argc, char **argv) {
	SlotDirectory  sd = SlotDirectory();
	sd.slots.push_back(new Slot(0, 3));
	sd.slots.push_back(new Slot(3, 4));
	sd.slots.push_back(new Slot(7, 3));

	void* data = malloc(sizeof(sd.getBytes()));
	sd.mapFromObject(data);

	SlotDirectory sd2 = SlotDirectory();
	sd2.mapToObject(data);

	assert(sd.getNumberOfSlots() == sd2.getNumberOfSlots());
	cout << sd.getNumberOfSlots() << sd2.getNumberOfSlots()  << endl;

//	cout << 0 << "- "<< sd.slots[0].offsetToRecord << ":"<< sd.slots[0].recordSize << endl;
//	cout << 0 << "- "<< sd2.slots[0].offsetToRecord << ":"<< sd2.slots[0].recordSize <<endl;
//
//	cout << 1 << "- "<< sd.slots[1].offsetToRecord << ":"<< sd.slots[1].recordSize << endl;
//	cout << 1 << "- "<< sd2.slots[1].offsetToRecord << ":"<< sd2.slots[1].recordSize <<endl;
//
//	cout << 2 << "- "<< sd.slots[2].offsetToRecord << ":"<< sd.slots[2].recordSize << endl;
//	cout << 2 << "- "<< sd2.slots[2].offsetToRecord << ":"<< sd2.slots[2].recordSize <<endl;
//
	Slot* sl1 = sd.slots.at(0);
	Slot* sl2 = sd2.slots.at(0);
//	cout << sl1 << endl;
	cout << sl2->recordSize << endl;
//	assert(sl1.offsetToRecord == sl2.offsetToRecord);
//	assert(sl1.recordSize == sl2.recordSize);
//
//	sl1 = *(sd.slots[1]);
//	sl2 = *(sd2.slots[1]);
//	assert(sl1.offsetToRecord == sl2.offsetToRecord);
//	assert(sl1.recordSize == sl2.recordSize);
//
//	sl1 = *(sd.slots[2]);
//	sl2 = *(sd2.slots[2]);
//	assert(sl1.offsetToRecord == sl2.offsetToRecord);
//	assert(sl1.recordSize == sl2.recordSize);

	cout << "All Test Cases Passed";

	return 0;
}
