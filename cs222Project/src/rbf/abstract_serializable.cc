#include "abstract_serializable.h"
#include <stdlib.h>
#include "file_util.h"
#include <stdio.h>

using namespace std;

int Serializable::deserialize(string fileName) {
	int size = fsize((char*)fileName.c_str());
	void* buffer = malloc(size);
	FILE* handle = fopen(fileName.c_str(), "rb") ;
	fread(buffer, size, 1, handle);
	this->mapToObject(buffer);
	free(buffer);
	fclose(handle);
	return 0;
}

int Serializable::serialize(string fileName) {
	int memory = this->getBytes();
	void* buffer = malloc(memory);
	this->mapFromObject(buffer);
	FILE* handle=fopen(fileName.c_str(), "wb");
	fwrite(buffer, memory, 1, handle);
	free(buffer);
	fclose(handle);
	return 0;
}

