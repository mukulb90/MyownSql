#include "pfm.h"
#include "page.h"
#include <string.h>

Page::Page() {
	this->data = (void *)malloc(PAGE_SIZE);
}

Page::Page(void * data) {
	this->data = data;
}

int Page::getBytes() {
	return PAGE_SIZE;
}

int Page::mapFromObject(void* data) {
	memcpy(data, this->data, PAGE_SIZE);
	return 0;
}

int Page::mapToObject(void* data) {
	memcpy(this->data, data, PAGE_SIZE);
	return 0;
}

Page::~Page() {
}
