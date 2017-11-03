
#include "ix.h"
#include <stack>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	int rc;
	PagedFileManager * pfm = PagedFileManager::instance();
	rc = pfm->createFile(fileName);
	if(rc == 0) {
		FileHandle fh;
		pfm->openFile(fileName, fh);
		Graph* graph = Graph::instance(fh);
		graph->serialize();
		pfm->closeFile(fh);
		return rc;
	}
	return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
	PagedFileManager * pfm = PagedFileManager::instance();
	return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	if(ixfileHandle.fileHandle != 0) {
		return -1;
	}
	PagedFileManager * pfm = PagedFileManager::instance();
	FileHandle* fh = new FileHandle();
	int rc = pfm->openFile(fileName, *fh);
	if(rc == 0) {
		ixfileHandle.fileHandle = fh;
	}
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	PagedFileManager * pfm = PagedFileManager::instance();
	int rc = pfm->closeFile(*ixfileHandle.fileHandle);
	if(rc == 0) {
		delete ixfileHandle.fileHandle;
		ixfileHandle.fileHandle = 0;
	}
	return rc;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int rc = 0;
	FileHandle fileHandle = *(ixfileHandle.fileHandle);
	if(fileHandle.file == 0) {
		return -1;
	}

	Graph* graph = Graph::instance(fileHandle, attribute);
	rc = graph->insertEntry(key, rid);
    return rc;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
	this->fileHandle = 0;
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = this->ixReadPageCounter;
	writePageCount = this->ixWritePageCounter;
	appendPageCount = this->ixAppendPageCounter;
    return 0;
}

Graph* Graph::instance(const FileHandle &fileHandle, const Attribute &attr){
	Graph* graph = Graph::instance(fileHandle);
	if(graph != 0) {
		graph->attr = attr;
	}
	return graph;
}

Graph* Graph::instance(const FileHandle &fileHandle) {
	Graph* graph = new Graph(fileHandle);

	// initialize a rootNode
	AuxiloryNode* root = new AuxiloryNode(0, fileHandle);
	graph->root = root;
	if(graph->root != 0) {
		return graph;
	}
	return 0;
}

Graph::Graph(const FileHandle &fileHandle) {
	this->root = 0;
	this->fileHandle = fileHandle;
}

RC Graph::serialize() {
	return this->root->serialize();
}

RC Graph::insertEntry(const void * key, RID const& rid) {
	NodeType nodeType;
	Node* node = this->root;
	stack<Node*> visitedNodes;
	visitedNodes.push(this->root);
	node->getNodeType(nodeType);

// traverse till leaf node
	while(nodeType != LEAF) {
		AuxiloryNode* auxiloryNode = (AuxiloryNode*)node;
		auxiloryNode->search(key, node);
		visitedNodes.push(this->root);
		node->getNodeType(nodeType);
	}

//	try inserting
	LeafNode* leafNode = (LeafNode*)node;
	if(leafNode != 0 && leafNode->insert(key) != -1) {
//		new key could fit nicely in existing thing, so do not need to do anything else
		leafNode->serialize();
		return 0;
	}
	if(leafNode == 0) {
		leafNode = new LeafNode(this->attr, this->fileHandle);
		leafNode->serialize();
	}

// for now , keep inserting in same node
	return -1;
}

AuxiloryNode::AuxiloryNode(const int &id, const Attribute &attr, const FileHandle &fileHandle):Node(id, attr, fileHandle) {

}

AuxiloryNode::AuxiloryNode(const int &id, const FileHandle & fileHandle): Node(id, fileHandle){

}

Node::Node(const int &id, const Attribute &attr, const FileHandle &fileHandle) {
	Node::Node(id, fileHandle);
	this-> attr = attr;
}

Node::Node(const int &id, const FileHandle &fileHandle) {
	this->id = id;
	this->fileHandle = fileHandle;
	this->data = malloc(PAGE_SIZE);
}

RC Node::deserialize() {
	return this->fileHandle.readPage(this->id, this->data);
}

RC Node::serialize() {
	return this->fileHandle.writePage(this->id, this->data);
}

RC Node::getNumberOfKeys(int& numberOfKeys) {
	char* cursor = (char*) this->data;
	memcpy(&numberOfKeys, cursor, sizeof(int));
	return 0;
}

RC Node::setNumberOfKeys(const int& numberOfKeys) {
	char* cursor = (char*) this->data;
	memcpy(cursor, &numberOfKeys, sizeof(int));
	return 0;
}

RC Node::getNodeType(NodeType& nodeType) {
	char* cursor = (char*) this->data;
	cursor += sizeof(int);
	memcpy(&nodeType, cursor, sizeof(int));
	return 0;
}

RC Node::setNodeType(const NodeType& nodeType) {
	char* cursor = (char*) this->data;
	cursor += sizeof(int);
	memcpy(cursor, &nodeType, sizeof(int));
	return 0;
}


RC AuxiloryNode::search(const void * key, Node* nextNode) {
	char* cursor = (char*)this->data;
	cursor += this->getMetaDataSize();
	char * tempCursor;
	int numberOfKeys;
	this->getNumberOfKeys(numberOfKeys);
	if(this->attr.type == TypeReal || this->attr.type == TypeInt) {
		for(int i=0; i < numberOfKeys; i++) {
			int nextPointer;
			int leftSubtreePointer = *((int*)cursor);
			cursor += sizeof(int);

			const void * firstKey =  cursor;
			cursor += sizeof(int);

			int midSubtreePointer =  *((int*)cursor);
			tempCursor = cursor;
			cursor += sizeof(int);

			const void * secondKey = cursor;
			cursor += sizeof(int);

			int rightSubtreePointer = *((int*) cursor);
			cursor += sizeof(int);

			if(i==0 && compare(key, firstKey, this->attr, false, false) >=0 ) {
				nextPointer = leftSubtreePointer;
			}
			else if(i == numberOfKeys-1 && compare(key, secondKey, this->attr, false, false) < 0) {
				nextPointer = rightSubtreePointer;
			}

			else if(compare(key, firstKey, this->attr, false, false) <= 0 &&
					compare(key, secondKey, this->attr, false, false) > 0) {
				nextPointer = midSubtreePointer;
			} else {
				cursor = tempCursor;
				continue;
			}
			Node* node = new Node(nextPointer, this->attr, this->fileHandle);
			node->deserialize();
			nextNode = node;
			return 0;
		}
	} else {
		//#TODO need to implement for varchar

	}
}


RC Node::getMetaDataSize() {
	return sizeof(int)*3;
}
