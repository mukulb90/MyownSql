#include <iostream>
#include <cassert>

#include "page.h"
using namespace std;

int main(int argc, char **argv) {
	Page* page = new Page();
	cout << page->getNumberOfSlots() << endl;
	cout << page->getFreeSpaceOffset() << endl;
	assert(page->getNumberOfSlots() == 0);
	assert(page->getFreeSpaceOffset() == 0);
	page->setFreeSpaceOffset(23);
	page->setNumberOfSlots(2);
	int offset, size;
	int pn=0, off=10, sz=2;
	page->setSlot(pn, off, sz);
	page->getSlot(pn, offset, size);
	assert(page->getFreeSpaceOffset() == 23);
	assert(page->getNumberOfSlots() == 2);
	assert(offset == 10);
	assert(size == 2);
	cout << "All Test Case Passed Successfully" << endl;
	return 0;
}
