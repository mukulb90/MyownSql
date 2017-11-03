#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

typedef enum  {
	ROOT = 0,
	AUXILORY,
	LEAF
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
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};


class IX_ScanIterator {
    public:

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
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class Node {
public:
	int id;
	void* data;
	Attribute attr;
	FileHandle fileHandle;

	Node(const int &id, const Attribute &attr, const FileHandle &fileHandle);
	Node(const int &id, const FileHandle &fileHandle);

	RC serialize();
	RC deserialize();

//	3 things which would be present in every node, lets call it node metadata
	RC setNodeType(const NodeType &type);
	RC getNodeType(NodeType &type);

	RC getNumberOfKeys(int &numberOfKeys);
	RC setNumberOfKeys(const int &numberOfKeys);

	RC setFreeSpace(const int &freeSpace);
	RC getFreeSpace(int &freeSapce);

	int getMetaDataSize();

};


class LeafNode: public Node {

public:
	LeafNode(const int &id, const Attribute &attr, const FileHandle &fileHandle);
	LeafNode(const Attribute &attr, const FileHandle &fileHandle);
	~LeafNode();

//	This method will try to insert the key in Leaf node, if not possible it will return -1
	RC insert(const void* value);
	RC setSibling(const int &pageNum);
	RC getSibling(int &pageNum);

};

class AuxiloryNode: public Node {

public:
	AuxiloryNode(const int &id, const Attribute &attr, const FileHandle &fileHandle);
	AuxiloryNode(const int &id, const FileHandle &fileHandle);
	~AuxiloryNode();

	RC getNumberOfChildNodes(int &numberOfNodes);
	RC search(const void * key, Node* nextNode);
	RC insertKey(void* key);
};

class Graph {

public:

	AuxiloryNode* root;
	Attribute attr;
	FileHandle fileHandle;

	Graph(const FileHandle &fileHandle);

	static Graph* instance(const FileHandle &fileHandle);
	static Graph* instance(const FileHandle &fileHandle,const Attribute &attr);

	RC getNodeByIndex(int index, Node &node);
	RC getNumberOfNodes(int &numberOfNodes);

	RC serialize();
	RC deserialize();
	RC insertEntry(const void *key, const RID &rid);
};

#endif
