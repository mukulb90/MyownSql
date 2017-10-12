#include "abstract_serializable.h"
#include <stdlib.h>
#include "file_util.h"
#include <stdio.h>

//using namespace std;

Serializable::~Serializable() {

}

int Serializable::deserialize(string fileName) {
	return this->deserializeToOffset(fileName, 0, 0);
}

int Serializable::serialize(string fileName) {
	return this->serializeToOffset(fileName, 0, 0);
}


int Serializable::deserializeToOffset(string fileName, int startOffset, int blockSize) {
	int size;
	if(blockSize == 0) {
		size = fsize((char*)fileName.c_str());
	} else {
		size = blockSize;
	}
	void* buffer = malloc(size);
	FILE* handle;
	handle = fopen(fileName.c_str(), "rb") ;

//	if(startOffset != 0) {
//		handle = fopen(fileName.c_str(), "rb") ;
//		fseek(handle, startOffset, SEEK_SET);
//
//	}
//	else {
//		handle = fopen(fileName.c_str(), "rb") ;
//	}
	fread(buffer, size, 1, handle);
	this->mapToObject(buffer);
	free(buffer);
	fclose(handle);
	return 0;
}

int Serializable::serializeToOffset(string fileName,  int startOffset=0, int blockSize=0) {
	int memory;
	if(blockSize == 0) {
		memory = this->getBytes();
	} else {
		memory = blockSize;
	}
	void* buffer = malloc(memory);
	this->mapFromObject(buffer);
	FILE* handle;
	if(startOffset != 0) {
		handle=fopen(fileName.c_str(), "rb+");
		fseek(handle, startOffset, SEEK_SET);
	}
	else {
		handle=fopen(fileName.c_str(), "wb");
	}
	fwrite(buffer, memory, 1, handle);
	free(buffer);
	fclose(handle);
	return 0;
}

