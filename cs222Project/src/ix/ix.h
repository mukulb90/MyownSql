#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;
class LeafNode;
class Node;
class AuxiloryNode;
class Graph;
class Entry;

typedef enum {
	ROOT = 0, AUXILORY, LEAF
} NodeType;

class IndexManager {

public:
	static IndexManager* instance();

	// Create an index file.
	RC createFile(const string &fileName);

	// Delete an index file.
	RC destroyFile(const string &fileName);

	// Open an index and return an ixfileHandle.
	RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

	// Close an ixfileHandle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle.
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given ixfileHandle.
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *key, const RID &rid);

	// Initialize and IX_ScanIterator to support a range search
	RC scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
			const void *lowKey, const void *highKey, bool lowKeyInclusive,
			bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree in pre-order (in a JSON record format)
	void printBtree(IXFileHandle &ixfileHandle,
			const Attribute &attribute) const;

protected:
	IndexManager();
	~IndexManager();

private:
	static IndexManager *_index_manager;
};

class IX_ScanIterator {

public:
	int numberOfElementsIterated;
	IXFileHandle* ixfileHandle;
	Attribute attribute;
	Entry* lowKeyEntry;
	Entry* highKeyEntry;

	CompOp lowOperator;
	CompOp highOperator;
	LeafNode* leafNode;
	Entry* currentEntry;

	// Constructor
	IX_ScanIterator();

	// Destructor
	~IX_ScanIterator();

	// Get next matching entry
	RC getNextEntry(RID &rid, void *key);

	// Terminate index scan
	RC close();
};

class IXFileHandle {
public:

	// variables to keep counter for each operation
	unsigned ixReadPageCounter;
	unsigned ixWritePageCounter;
	unsigned ixAppendPageCounter;
	FileHandle* fileHandle;
	// Constructor
	IXFileHandle();

	// Destructor
	~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
			unsigned &appendPageCount);

};



class Entry {
public:
	void* data;
	Attribute attr;

	virtual const void* getKey() = 0;
	virtual int getEntrySize() = 0;
	virtual int getSpaceNeededToInsert() = 0;
    int getPageNum();
    int getSlotNum();

	friend bool operator <(Entry& entry, Entry& entry2);
	friend bool operator <=(Entry& entry, Entry& entry2);

	friend bool operator >(Entry &entry, Entry &entry2);

	friend bool operator >=(Entry& entry, Entry& entry2);
	friend bool operator ==(Entry& entry, Entry& entry2);
	virtual Entry* getNextEntry() = 0;
	virtual string toJson() = 0;
};


class LeafEntry: public Entry {

public:

	LeafEntry(void* data, Attribute &attr);
	const void* getKey();
	int getEntrySize();
	int getSpaceNeededToInsert();

	static LeafEntry* parse(Attribute &attr,const void* key, const int &pageNum,const int &slotNum);
	static int getSize(Attribute &attr,const void* key);
	RC unparse(Attribute &attr, void* key, int &pageNum, int &slotNum);
	Entry* getNextEntry();
	string toJson();
	void setPageNum(int &pageNum);
	void setSlotNum(int &slotNum);
	int getPageNum();
	int getSlotNum();
};


class AuxiloryEntry: public Entry {

public:
	AuxiloryEntry(void* data, Attribute &attr);

	const void* getKey();
	int getEntrySize();
	int getSpaceNeededToInsert();
	Entry* getNextEntry();

	static AuxiloryEntry* parse(Attribute &attr,const void* key,const int pageNum,const int slotNum, const int &rightPointer);
	static int getSize(Attribute &attr, void* key);
	RC unparse(Attribute &attr, void* key, int &pageNum, int &slotNum, int &rightPointer);
	string toJson();

	void setRightPointer(int &rightPointer);
	int getLeftPointer();
	int getRightPointer();
	void setLeftPointer(int &leftPointer);
};

class Node {

protected:
	//	This constructor is used to create a node if nodeId is known
	Node(const int &id, const Attribute &attr, const FileHandle &fileHandle);
	Node(const int &id, const FileHandle &fileHandle);
public:

	int id;
	void* data;
	Attribute attr;
	FileHandle fileHandle;

//	This constructor is used to create a new Node
	Node(const Attribute &attr, const FileHandle &fileHandle);

	~Node();

	static Node* getInstance(const int &id, const Attribute &attr, const FileHandle &fileHandle);

	RC insertEntry(Entry* entry);
	RC internalInsert(void* cursor, Entry* entry);

// 	save the node on file system
	RC serialize();

//	read the node from file system
	RC deserialize();

//	3 things which would be present in every node, lets call it node metadata
	RC setNodeType(const NodeType &type);
	RC getNodeType(NodeType &type);

	RC getNumberOfKeys(int &numberOfKeys);
	RC setNumberOfKeys(const int &numberOfKeys);

	RC setFreeSpace(const int &freeSpace);
	RC getFreeSpace(int &freeSapce);
	Entry* getFirstEntry();

	int getMetaDataSize();

	RC deleteEntry(Entry * deleteEntry);
};

class LeafNode: public Node {

public:
	LeafNode(int id, const FileHandle &fileHandle);
	LeafNode(const Attribute &attr, const FileHandle &fileHandle);
	LeafNode(const int &id, const Attribute &attr, const FileHandle &fileHandle);
	~LeafNode();

	//	This method splits the Node into two splits and equally distributes the entries between both nodes
	RC split(Node* secondNode, AuxiloryEntry* &entryPointingToBothNodes);

	//RC deleteEntry(Entry* entry);
	RC setLeftSibling(const short &pageNum);
	RC getLeftSibling(short &pageNum);
	RC setRightSibling(const short &pageNum);
	RC getRightSibling(short &pageNum);
	RC addAfter(LeafNode *node);
	RC addBefore(LeafNode* node);
	string toJson();
//	RC deleteEntry(Entry * deleteEntry);

};

class AuxiloryNode: public Node {

public:
	AuxiloryNode(const Attribute &attr,
				const FileHandle &fileHandle);

	AuxiloryNode(const int &id, const Attribute &attr,
			const FileHandle &fileHandle);
	AuxiloryNode(const int &id, const FileHandle &fileHandle);

	~AuxiloryNode();

	RC split(Node* secondNode, AuxiloryEntry* &entryPointingToBothNodes);

	Entry* search(Entry * searchEntry, Node* &nextNode);
//	RC deleteEntry(Entry* entry);
	string toJson();
	int getLeftPointer();
	RC setLeftPointer(const int &leftPointer);
};

class Graph {
public:
    AuxiloryNode* internalRoot;
//    AuxiloryNode* root;
	Attribute attr;
	FileHandle fileHandle;

	Graph(const FileHandle &fileHandle);
	Graph(const FileHandle &fileHandle, const Attribute& attr);
	~Graph();

	static Graph* instance(const FileHandle &fileHandle);
	static Graph* instance(const FileHandle &fileHandle, const Attribute &attr);

    Node* getRoot();
    void setRoot(AuxiloryNode* root);
	void* getMinKey();
	void *getMaxKey();
	RC serialize();
	RC deserialize();
	RC insertEntry(const void *key, const RID &rid);
	RC deleteKey(const void *key, const RID &rid);
};

#endif
