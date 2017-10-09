#include <cassert>
#include <iostream>

#include "slot.h"

using namespace std;

int main(int argc, char **argv) {
	Slot s  = Slot(3, 6);
	void* data = malloc(s.getBytes());
	s.mapFromObject(data);
	Slot s1 = Slot();
	s1.mapToObject(data);
	assert(s.offsetToRecord == s1.offsetToRecord);
	assert(s.recordSize == s1.recordSize);
	cout << "All test cases passed successfully";
	return 0;
}
