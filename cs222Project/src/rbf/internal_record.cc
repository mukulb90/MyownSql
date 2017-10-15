#include <math.h>
#include <string.h>
#include<stdlib.h>
#include <iostream>

#include "internal_record.h"
#include <vector>

#include "page.h"


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
	size += numberOfNullBytes;
	size += sizeof(unsigned short)*(recordDescriptor.size()+1);


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
	void* internalData = malloc(InternalRecord::getInternalRecordBytes(recordDescriptor, data));
	char * startInternalCursor = (char *)internalData;
	char * internalCursor = (char *)internalData;

	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	memcpy(internalData, cursor, numberOfNullBytes);
	internalCursor += numberOfNullBytes;
	cursor += numberOfNullBytes;

//	Data start from here
	int numberOfAttributes = recordDescriptor.size();
	unsigned short insertionOffset = (unsigned short)numberOfNullBytes + (unsigned short)sizeof(unsigned short)*(numberOfAttributes+1);

	for(int i=0; i<numberOfAttributes; i++){
		Attribute attr = recordDescriptor[i];
		if(*(nullBits+i)){
			memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));
			continue;
		}
		if (attr.type == TypeInt || attr.type == TypeReal) {
			memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));

			memcpy(startInternalCursor + insertionOffset, cursor, 4);
			insertionOffset += (unsigned short)4;
			cursor += 4;
		} else {
		int length = *cursor;
		cursor += 4;
		memcpy(internalCursor + i*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));
		memcpy(startInternalCursor + insertionOffset, cursor, length);
		cursor += length;
		insertionOffset += length;
		}
	}

	memcpy(internalCursor + numberOfAttributes*sizeof(unsigned short), &insertionOffset, sizeof(unsigned short));

	record->data = internalData;
	return record;
}


RC InternalRecord::unParse(const vector<Attribute> &recordDescriptor, void* data) {
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	char * startInternalCursor = (char *) this->data;
	char * internalCursor = (char*)this->data;
	char * cursor = (char *)data;

	vector<bool> nullIndicator;
	vector<int> lengthOfAttributes;

	 memcpy(cursor, internalCursor, numberOfNullBytes);
	 cursor += numberOfNullBytes;
	 internalCursor += numberOfNullBytes;

	for (int i = 0; i < recordDescriptor.size(); i++) {
		unsigned short startOffset = *(unsigned short*)(internalCursor+i*(sizeof(unsigned short)));
		unsigned short endOffSet = *(unsigned short *)(internalCursor+((i+1)*(sizeof(unsigned short))));
		int length = endOffSet - startOffset;
//		cout << i << "-" << length <<endl;
		lengthOfAttributes.push_back(length);
		if(length == 0){
			nullIndicator.push_back(1);
		}
		else {
			nullIndicator.push_back(0);
		}
	}


	for(int i=0; i< recordDescriptor.size(); i++){
		Attribute attr = recordDescriptor[i];
		if(nullIndicator[i]){
			continue;
		}
		unsigned short offsetFromStart =  *(unsigned short*)(internalCursor+i*(sizeof(unsigned short)));
		if (attr.type == TypeInt || attr.type == TypeReal) {
			memcpy(cursor, startInternalCursor + offsetFromStart, 4);
			cursor += 4;
		} else {

		int length = lengthOfAttributes[i];
		memcpy(cursor, &length, 4);
		cursor += 4;

		memcpy(cursor, startInternalCursor + offsetFromStart , length);
		cursor += length;
		}
	}
	return 0;
}

RC InternalRecord::getAttributeByIndex(const int &index, const vector<Attribute> &recordDescriptor, void * attribute) {
	char * cursor = (char *)this->data;
	char * startCursor = (char *)this->data;
	int numberOfNullBytes = getNumberOfNullBytes(recordDescriptor);
	cursor += numberOfNullBytes;
	unsigned short offsetFromStart = *(unsigned short*)(cursor + sizeof(unsigned short)*index);
	unsigned short nextOffsetFromStart = *(unsigned short*)(cursor + sizeof(unsigned short)*(index+1));
	unsigned short numberOfBytes = nextOffsetFromStart - offsetFromStart;
	memcpy(attribute, startCursor + offsetFromStart, numberOfBytes);
	return 0;
}
