#ifndef _abstract_serializable_h_
#define _abstract_serializable_h_

#include<stdio.h>
#include<string>

using namespace std;

class Serializable {
public :

	virtual ~Serializable();
	int deserialize(string fileName);
	int serialize(string fileName);
	int deserializeToOffset(string fileName, int start, int size);
	int serializeToOffset(string fileName,  int startOffset, int size);

	virtual int getBytes() = 0;
	virtual int mapFromObject(void* data) = 0;
	virtual int mapToObject(void* data) = 0;
};

#endif

