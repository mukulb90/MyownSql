#ifndef _abstract_serializable_h_
#define _abstract_serializable_h_

#include<stdio.h>
#include<string>

using namespace std;

class Serializable {
public :

	int deserialize(string fileName);
	int serialize(string fileName);

	virtual int getBytes() = 0;
	virtual int mapFromObject(void* data) = 0;
	virtual int mapToObject(void* data) = 0;
};

#endif

