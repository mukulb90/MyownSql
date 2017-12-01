
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:

	RBFM_ScanIterator* rbfmIterator;
	RM_ScanIterator() {
	  this->rbfmIterator = 0;
	};
  ~RM_ScanIterator() {
	  if(this->rbfmIterator!=0)
	  free(this->rbfmIterator);
  };

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() {};  	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key) {return RM_EOF;};  	// Get next matching entry
  RC close() {return -1;};             			// Terminate index scan
};


// Relation Manager
class RelationManager
{
private:
    FileHandle* fileHandle;
    
public:

  unordered_map<string, vector<Attribute>> tableNameToRecordDescriptorMap;
  unordered_map<string, vector<vector<Attribute>>> tableNameToRecordDescriptorsMap;

  static RelationManager* instance();

  static bool isSystemTable(const string &tableName);

  RC invalidateCache(const string &tableName);

  RC createCatalog();

  RC deleteCatalog();

  FileHandle*  getFileHandle();

  void printTable(string tableName);

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getTableDetailsByName(const string &tableName, int &tableId, int &versionId);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC getAttributesVector(const string &tableName, vector<vector<Attribute>> &recordDescriptors);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

	RC internalInsertTuple(const string &tableName, const void *data, RID &rid);

    RC internalDeleteTuple(const string &tableName, const RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

	RC internalUpdateTuple(const string &tableName, const void *data, const RID &rid);

	RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

  RC internalAddAttribute(const string &tableName, const Attribute &attr);

  RC internalDropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

};

class TableCatalogRecord {

public:
	TableCatalogRecord(void *);
	~TableCatalogRecord();

	void* data;

	static TableCatalogRecord* parse(const int &id, const string &tableName, const int &version);
	RC unParse(int &id, string &tableName);
};


class ColumnsCatalogRecord {

public:
	ColumnsCatalogRecord(void *);
	~ColumnsCatalogRecord();

	void* data;

	static ColumnsCatalogRecord* parse(const int &tableId, const Attribute &attrs, const int &columnIndex, const int &version);
	RC unParse(int &tableId, Attribute &attrs, int &columnIndex, int &version);
};

#endif
