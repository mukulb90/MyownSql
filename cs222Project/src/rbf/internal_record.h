#ifndef _internal_record_h_
#define _internal_record_h_

#import <vector>

#import "common.h"

using namespace std;

class InternalRecord {

private:

public :
	void* data;

	InternalRecord();
	static int getInternalRecordBytes(const vector<Attribute> &recordDescriptor, const void* data);
	static InternalRecord* parse(const vector<Attribute> &recordDescriptor,const void* data);
	RC unParse(const vector<Attribute> &recordDescriptor, void* data, int size);
	RC getBytes();

};

#endif

