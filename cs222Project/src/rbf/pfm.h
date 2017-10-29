#ifndef _pfm_h_
#define _pfm_h_

#include <string>
#include <climits>
#include <stdio.h>
#include <vector>
#include <sstream>
#include <iostream>
#include <math.h>

#define FILE_HANDLE_SERIALIZATION_LOCATION  "access_stats.data";

using namespace std;

#include <string.h>
#include <climits>
#define PAGE_SIZE 4096
#define FORWARDER_SIZE 12

using namespace std;

// Record ID
typedef struct
{
  unsigned pageNum;    // page number
  unsigned slotNum;    // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// =
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

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


class InternalRecord {

private:

public :
	void* data;

	InternalRecord();
	static int getInternalRecordBytes(const vector<Attribute> &recordDescriptor, const void* data);
	static InternalRecord* parse(const vector<Attribute> &recordDescriptor,const void* data, const int &versionId);
	RC unParse(const vector<Attribute> &recordDescriptor, void* data, int &versionId);
	RC getBytes();
	RC getAttributeByIndex(const int &index, const vector<Attribute> &recordDescriptor, void* attribute, bool &isNull);
	RC getVersionId(int &versionId);


};

class RecordForwarder {

private:

public :
	void* data;
	int pageNum;
	int slotNum;
	bool isForwarderFlag = false;
	RecordForwarder(RID rid);
	RecordForwarder ();
//	RecordForwarder (RID rid,bool isForwarderFlag);
	int getInternalRecordBytes(const vector<Attribute> &recordDescriptor,const void* data, bool isForwardFlag);
	static RecordForwarder*  parse(const vector<Attribute> &recordDescriptor,const void* data, RID rid,bool isForwarderFlag, const int &versionId);
	RC unparse(const vector<Attribute> &recordDescriptor, void* data, int &versionId);
	void setForwarderValues(int &forwarder,int &pageNum, int &slotNum, RID rid);
	void getForwarderValues(int &forwarder,int &pageNum, int &slotNum);
	InternalRecord* getInternalRecData();
	bool isDataForwarder(int &pageNum, int &slotNum);
};


class Page: public Serializable {
public:
	void * data;

	Page();
	Page(void * data);
	~Page();

	RecordForwarder* getRecord(const RID &rid);
	int getBytes();
	int mapFromObject(void* data);
	int mapToObject(void* data);
	int getFreeSpaceOffset();
	void setFreeSpaceOffset(int);
	int getFreeSpaceOffSetPointer();
	int getNumberOfSlots();
	void setNumberOfSlots(int);
	int getNumberOfSlotsPointer();
	int getAvailableSpace();
	RC insertRecord(const vector<Attribute> &recordDescriptor, const void *data, RID &rid, const int &versionId);
	static int getRecordSize(const vector<Attribute> &recordDescriptor, const void *data);
	RC setSlot(int &index, int &offset, int &size);
	RC getSlot(int &index, int &offset, int &size);

};

class PagedFile;

class FileHandle : public Serializable{

public:
	// variables to keep the counter for each operation
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;
	PagedFile * file;
	FILE * serializerHandle;
	FILE * deserialerHandle;

	FileHandle();                                         // Default constructor
	~FileHandle();                                                 // Destructor

	RC internalReadPage(PageNum pageNum, void *data, bool shouldAffectCounters);
	RC readPage(PageNum pageNum, void *data);             // Get a specific page
	RC writePage(PageNum pageNum, const void *data);    // Write a specific page
	RC appendPage(const void *data);                   // Append a specific page
	unsigned getNumberOfPages();          // Get the number of pages in the file
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
			unsigned &appendPageCount); // Put the current counter values into variables

	void setPagedFile(PagedFile *);
	int getBytes();
	int mapFromObject(void* data);
	int mapToObject(void* data);
};

class PagedFile: public Serializable {
private:

public:
	string name;
	int numberOfPages;
	vector<Page *> pages;
	FileHandle* handle;


	PagedFile(string fileName);
	~PagedFile();

	Page* getPageByIndex(int index);
	int getBytes();
	int getNumberOfPages();
	int mapFromObject(void* data);
	int mapToObject(void* data);
	int getPageStartOffsetByIndex(int num);
	int getPageMetaDataSize();
	int setFileHandle(FileHandle *fileHandle);
};


class PagedFileManager {
public:
	static PagedFileManager* instance();   // Access to the _pf_manager instance

	RC createFile(const string &fileName);                  // Create a new file
	RC destroyFile(const string &fileName);                    // Destroy a file
	RC openFile(const string &fileName, FileHandle &fileHandle);  // Open a file
	RC closeFile(FileHandle &fileHandle);                        // Close a file

protected:
	PagedFileManager();                                           // Constructor
	~PagedFileManager();                                           // Destructor

private:
	static PagedFileManager *_pf_manager;
};


bool fileExists(string fileName);

unsigned long fsize(char * fileName);

int getNumberOfNullBytes(const vector<Attribute> &recordDescriptor);
bool* getNullBits(const vector<Attribute> &recordDescriptor, const void* data);

class VarcharParser {
private:
	VarcharParser();
public:
	void* data;
	~VarcharParser();

	static VarcharParser* parse(const string &str);
	RC unParse(string &str);
};

#endif
