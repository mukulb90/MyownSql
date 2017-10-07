#include "abstract_serializable.h"
#include <stdio.h>
#include <stdlib.h>
#include "file_util.h"

int Serializable::deserialize(FILE* handle) {
	int size = fsize(handle);
	void* buffer = malloc(size);
	fread(buffer, size, 1, handle);
	this->mapToObject(buffer);
	return 0;
}

int Serializable::serialize(FILE* handle) {
	int memory = this->getBytes();
	void* buffer = malloc(memory);
	this->mapFromObject(buffer);
	fwrite(buffer, memory, 1, handle);
	fflush(handle);
	return 0;
}

