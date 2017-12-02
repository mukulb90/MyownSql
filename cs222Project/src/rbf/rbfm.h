#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <unordered_set>

#include "../rbf/pfm.h"

/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
	FileHandle fileHandle;
	vector<vector<Attribute>> recordDescriptors;
	Attribute conditionAttribute;
	CompOp compOp;                  // comparision type such as "<" and "="
	const void* value;
	vector<string> attributeNames;
	int pageNumber;
	int slotNumber;
	int versionId;
	unordered_set<RID, hashRID> alreadyVisitedRids;

  RBFM_ScanIterator();
  ~RBFM_ScanIterator();

  // Never keep the results in the memory. When getNextRecord() is called, 
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  RC close() { return 0; };
};


class RecordBasedFileManager
{
public:
  static RecordBasedFileManager* instance();

  RC createFile(const string &fileName);
  
  RC destroyFile(const string &fileName);
  
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  RC closeFile(FileHandle &fileHandle);

  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);
  RC internalInsertRecord(FileHandle &fileHandle, const vector<vector<Attribute>> &recordDescriptor, const void *data, RID &rid, const int &versionId, const int &isPointedByForwarder);

  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  RC internalReadRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data, int &versionId);
  
  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);
  RC internalUpdateRecord(FileHandle &fileHandle, const vector<vector<Attribute>> &recordDescriptors, const void *data, const RID &rid, const int &versionId);

  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);
  RC internalReadAttribute(FileHandle &fileHandle, const vector<vector<Attribute>> &recordDescriptors, const RID &rid, const string &attributeName, void *data, int &versionId);

  RC shiftNUpdateRecord(Page &page, int threshold,
  		int slotNumber, int rOffset, int rSize, int pageSlots, int recordSize,
  		 RecordForwarder *recordForwarder, FileHandle &fileHandle);

  RC updateSlotDir(int &currRecordOffset, int &currRecordSize, Page &page, int pageSlots);
  RC shiftRecords(int &currRecordOffset, int &currRecordSize, Page &page);

  RC readAttributes(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const vector<string> &attributeName, void *data);
  RC internalReadAttributes(FileHandle &fileHandle, const vector<vector<Attribute>> &recordDescriptors, const RID &rid, const vector<string> &attributeName, void *data, const int versionId);

  RC getInternalRecData(const vector<Attribute> &recordDescriptor, FileHandle &fileHandle);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

  RC internalScan(FileHandle &fileHandle,
        const vector<vector<Attribute>> &recordDescriptors,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator,
		const int &versionId);

public:

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;
};

void mergeAttributesData(vector<Attribute> newRecordDescriptorForProjections,
	vector<bool> isNullArray, vector<void*> dataArray, void* data);

#endif

