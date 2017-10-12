#include <math.h>
#include <string.h>
#include<stdlib.h>
#include <iostream>

#include "internal_record.h"
#include <vector>

InternalRecord::InternalRecord(){
	this->data = 0;
}

int getNumberOfNullBytes(const vector<Attribute> &recordDescriptor){
	return ceil(recordDescriptor.size() / 8.0);
}

bool* getNullBits(const vector<Attribute> &recordDescriptor, const void* data) {
	int nullBytesSize = getNumberOfNullBytes(recordDescriptor);
	unsigned char* nullstream  = (unsigned char*)malloc(nullBytesSize);
		memcpy(nullstream, data, nullBytesSize);
//# TODO free
		bool * nullarr = (bool *)malloc(recordDescriptor.size());

		for(int i=0;  i<recordDescriptor.size(); i++){
			int k = int(i/8);
			*(nullarr+i) = nullstream[nullBytesSize-1-k] & (1<<((nullBytesSize*8)-1-i));
		}
		free(nullstream);
		return nullarr;
}

int InternalRecord::getInternalRecordBytes(const vector<Attribute> &recordDescriptor,const void* data){
	int size = 0;
	char * cursor = (char *)data;
	bool * nullBits = getNullBits(recordDescriptor, data);
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);

	cursor += numberOfNullBytes;
	size += sizeof(unsigned short)*recordDescriptor.size();



	for (int i = 0; i < recordDescriptor.size(); i++) {
			Attribute attr = recordDescriptor[i];
			if(nullBits[i]){
				continue;
			}
			if (attr.type == TypeInt || attr.type == TypeReal) {
				size += 4;
				cursor += 4;
			} else {
				int length = *cursor;
				cursor += 4;
				cursor += length;
				size += length;
			}

	}

	return size;
}

InternalRecord* InternalRecord::parse(const vector<Attribute> &recordDescriptor,const void* data){
	InternalRecord* record = new InternalRecord();
	bool * nullBits = getNullBits(recordDescriptor, data);
	char * cursor = (char *)data;

	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	cursor += numberOfNullBytes;

//	Data start from here

	int numberOfAttributes = recordDescriptor.size();
	unsigned short insertionOffset = numberOfAttributes*sizeof(unsigned short);
	//# TODO free
	void* internalData = malloc(InternalRecord::getInternalRecordBytes(recordDescriptor, data));
	char* internalCursor = (char*) internalData;
	for(int i=0; i<numberOfAttributes; i++){
		Attribute attr = recordDescriptor[i];
		if(*(nullBits+i)){
			memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));
			continue;
		}
		if (attr.type == TypeInt || attr.type == TypeReal) {
			memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));
			memcpy(internalCursor + insertionOffset, cursor, 4);
			insertionOffset += 4;
			cursor += 4;
		} else {
		int length = *cursor;
		cursor += 4;
		memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));
		memcpy(internalCursor + insertionOffset, cursor, length);
		cursor += length;
		insertionOffset += length;
		}
	}

	record->data = internalData;
	char * ref = (char *)record->data;
	for (int i = 0; i < recordDescriptor.size(); ++i) {
		Attribute attr = recordDescriptor[i];
		if (attr.type == TypeInt || attr.type == TypeReal) {
		}
		else {

		}
	}
	return record;
}


RC InternalRecord::unParse(const vector<Attribute> &recordDescriptor, void* data, int size) {
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	char * internalCursor = (char*)this->data;

	vector<bool> nullIndicator;
	vector<int> lengthOfAttributes;
	for (int i = 0; i < recordDescriptor.size()-1; i++) {
		unsigned short startOffset = *(unsigned short*)(internalCursor+i*2);
		unsigned short endOffSet = *(unsigned short*)(internalCursor+(i+1)*2);
		int length = endOffSet - startOffset;
		lengthOfAttributes.push_back(length);
		if(length == 0){
			nullIndicator.push_back(1);
		}
		else {
			nullIndicator.push_back(0);
		}
	}

	// getNullbit for last element
	unsigned short startOffset = *(unsigned short*)(internalCursor + (recordDescriptor.size()-1)*2);
	unsigned short endOffset = size;
	int length = endOffset - startOffset;
	lengthOfAttributes.push_back(length);

	if(length == 0) {
		nullIndicator.push_back(1);
	}
	else {
		nullIndicator.push_back(0);
	}

	char * cursor = (char * )data;
	for (int i = 0; i < numberOfNullBytes; ++i) {
		 unsigned char byte = 0;
		 for (int i=0; i < 8; ++i){
			 if (nullIndicator[i]){
				byte |= 1 << i;
			 }
		 }
	 memcpy(cursor, &byte, sizeof(unsigned char));
	 cursor += sizeof(unsigned char);
	}

	internalCursor = (char*)this->data;


	for(int i=0; i< recordDescriptor.size(); i++){
		Attribute attr = recordDescriptor[i];
		if(nullIndicator[i]){
			continue;
		}
		unsigned short startOffset =  *(unsigned short*)(internalCursor+i*2);
		if (attr.type == TypeInt || attr.type == TypeReal) {
			memcpy(cursor, internalCursor + startOffset, 4);
			cursor += 4;
		} else {

		int length = lengthOfAttributes[i];
		memcpy(cursor, &length, 4);
		cursor += 4;

		memcpy(cursor, internalCursor + startOffset , length);
		cursor += length;
		}
	}
	return 0;
}


