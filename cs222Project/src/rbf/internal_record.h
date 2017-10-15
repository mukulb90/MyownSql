#ifndef _internal_record_h_
#define _internal_record_h_

#include <vector>

#include "common.h"

using namespace std;

class InternalRecord {

private:

public :
	void* data;

	InternalRecord();
	static int getInternalRecordBytes(const vector<Attribute> &recordDescriptor, const void* data);
	static InternalRecord* parse(const vector<Attribute> &recordDescriptor,const void* data);
	RC unParse(const vector<Attribute> &recordDescriptor, void* data);
	RC getBytes();
	RC getAttributeByIndex(const int &index, const vector<Attribute> &recordDescriptor, void* attribute);

};

#endif

