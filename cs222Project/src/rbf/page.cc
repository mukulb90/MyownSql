#include "pfm.h"
#include "page.h"

Page::Page(int pageId) {
	this->pageId = pageId;
}

int Page::getBytes() {
	return PAGE_SIZE;
}

int Page::mapFromObject(void* data) {
	return -1;
}

int Page::mapToObject(void* data) {
	return -1;
}
