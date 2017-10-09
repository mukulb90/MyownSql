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
	return 0;
}
