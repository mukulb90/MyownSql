#include "ix.h"
#include <stack>

#define INVALID_POINTER -1

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
	int rc;
	PagedFileManager * pfm = PagedFileManager::instance();
	rc = pfm->createFile(fileName);
	if (rc == 0) {
		FileHandle fh;
		pfm->openFile(fileName, fh);
		Graph* graph = Graph::instance(fh);
		rc = graph->serialize();
		pfm->closeFile(fh);
		return rc;
	}
	return rc;
}

RC IndexManager::destroyFile(const string &fileName) {
	PagedFileManager * pfm = PagedFileManager::instance();
	return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	if (ixfileHandle.fileHandle != 0) {
		return -1;
	}
	PagedFileManager * pfm = PagedFileManager::instance();
	FileHandle* fh = new FileHandle();
	int rc = pfm->openFile(fileName, *fh);
	if (rc == 0) {
		ixfileHandle.fileHandle = fh;
	}
	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	PagedFileManager * pfm = PagedFileManager::instance();
	int rc = pfm->closeFile(*ixfileHandle.fileHandle);
	if (rc == 0) {
		delete ixfileHandle.fileHandle;
		ixfileHandle.fileHandle = 0;
	}
	return rc;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
	int rc = 0;
	FileHandle fileHandle = *(ixfileHandle.fileHandle);
	if (fileHandle.file == 0) {
		return -1;
	}

	Graph* graph = new Graph(fileHandle, attribute);
	rc = graph->deserialize();
	if (rc != 0) {
		return rc;
	}

	rc = graph->insertEntry(key, rid);
	if (rc == 0) {
		ixfileHandle.ixWritePageCounter++;
	}
	return rc;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
	return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
	ix_ScanIterator.ixfileHandle = &ixfileHandle;
	ix_ScanIterator.lowKey = (void*) lowKey;
	ix_ScanIterator.highKey = (void*) highKey;
	ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;
	ix_ScanIterator.attribute = attribute;
	FileHandle* fileHandle = ixfileHandle.fileHandle;
	Graph* graph = new Graph(*fileHandle, attribute);
	int rc = graph->deserialize();
	if (rc == -1) {
		return rc;
	}
	Node* node = graph->root;
	Node* nextNode = 0;
	NodeType nodeType;
	node->getNodeType(nodeType);
	while (nodeType != LEAF) {
		AuxiloryNode* auxNode = (AuxiloryNode*) node;
		auxNode->search(lowKey, nextNode);
		nextNode->getNodeType(nodeType);
	}
	LeafNode* leafNode = (LeafNode*) nextNode;
	ix_ScanIterator.leafNode = leafNode;
	ix_ScanIterator.numberOfElementsIterated = 0;
	ix_ScanIterator.cursor = (char*) leafNode->data;
	return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
		const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

void IX_ScanIterator::getRIDAndKey(char* from, RID& rid, void*key) {
	if (this->attribute.type == TypeInt) {

		memcpy(key, from, sizeof(int));
		from += sizeof(int);

		memcpy(&(rid.pageNum), from, sizeof(int));
		from += sizeof(int);

		memcpy(&rid.slotNum, from, sizeof(int));
		from += sizeof(int);

	} else if (this->attribute.type == TypeReal) {
		memcpy(key, from, sizeof(float));
		from += sizeof(float);

		memcpy(&rid.pageNum, from, sizeof(int));
		from += sizeof(int);

		memcpy(&rid.slotNum, from, sizeof(int));
		from += sizeof(int);
	} else {
	}
	this->ixfileHandle->ixReadPageCounter++;
	this->cursor = from;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
	cursor += this->leafNode->getMetaDataSize();
	int numberOfKeys;
	this->leafNode->getNumberOfKeys(numberOfKeys);
	while (this->numberOfElementsIterated < numberOfKeys) {
		if (lowKey == NULL or highKey == NULL) {
			this->numberOfElementsIterated++;
			this->getRIDAndKey((char*) this->cursor, rid, key);
			return 0;
		} else if (this->lowKeyInclusive && this->highKeyInclusive) {
			if (compare(this->lowKey, cursor, this->attribute, false, false) <= 0
					&& compare(this->highKey, cursor, this->attribute, false,
							false) >= 0) {
				this->numberOfElementsIterated++;
				this->getRIDAndKey((char*) cursor, rid, key);
				return 0;
			}
		} else if (this->lowKeyInclusive && !this->highKeyInclusive) {
			if (compare(this->lowKey, cursor, this->attribute, false, false) <= 0
					&& compare(this->highKey, cursor, this->attribute, false,
							false) > 0) {
				this->numberOfElementsIterated++;
				this->getRIDAndKey((char*) cursor, rid, key);
				return 0;
			}
		} else if (!this->lowKeyInclusive && this->highKeyInclusive) {
			if (compare(this->lowKey, cursor, this->attribute, false, false) < 0
					&& compare(this->highKey, cursor, this->attribute, false,
							false) >= 0) {
				this->numberOfElementsIterated++;
				this->getRIDAndKey((char*) cursor, rid, key);
				return 0;
			} else if (!this->lowKeyInclusive && !this->highKeyInclusive) {
				if (compare(this->lowKey, cursor, this->attribute, false, false)
						< 0
						&& compare(this->highKey, cursor, this->attribute, false,
								false) > 0) {
					this->numberOfElementsIterated++;
					this->getRIDAndKey((char*) cursor, rid, key);
					return 0;
				}
			}
		}
	}
	return -1;
}

RC IX_ScanIterator::close() {
	if(this->ixfileHandle->fileHandle->file == 0){
		return -1;
	}
	return PagedFileManager::instance()->closeFile(*this->ixfileHandle->fileHandle);
}

IXFileHandle::IXFileHandle() {
	this->fileHandle = 0;
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	readPageCount = this->ixReadPageCounter;
	writePageCount = this->ixWritePageCounter;
	appendPageCount = this->ixAppendPageCounter;
	return 0;
}

Graph* Graph::instance(const FileHandle &fileHandle, const Attribute &attr) {
	Graph* graph = Graph::instance(fileHandle);
	if (graph != 0) {
		graph->attr = attr;
	}
	return graph;
}

Graph* Graph::instance(const FileHandle &fileHandle) {
	Graph* graph = new Graph(fileHandle);

	// initialize a rootNode
	AuxiloryNode* root = new AuxiloryNode(0, fileHandle);
	root->setNumberOfKeys(0);
	graph->root = root;
	if (graph->root != 0) {
		return graph;
	}
	return 0;
}

Graph::Graph(const FileHandle &fileHandle) {
	this->root = new AuxiloryNode(0, fileHandle);
	this->root->setNodeType(ROOT);
	this->fileHandle = fileHandle;
}

Graph::Graph(const FileHandle &fileHandle, const Attribute& attr) {
	this->root = new AuxiloryNode(0, attr, fileHandle);
	this->attr = attr;
	this->fileHandle = fileHandle;
	this->root->setNodeType(ROOT);
}

RC Graph::serialize() {
	return this->root->serialize();
}

RC Graph::deserialize() {
	return this->root->deserialize();
}

RC Graph::insertEntry(const void * key, RID const& rid) {
	NodeType nodeType;
	Node* node = this->root;
	stack<Node*> visitedNodes;
	visitedNodes.push(this->root);
	node->getNodeType(nodeType);

	int numberOfKeys;
	node->getNumberOfKeys(numberOfKeys);
	cout << "numberOfKeys: " << numberOfKeys << endl;
	if (numberOfKeys == 0) {
		LeafNode* leafNode = new LeafNode(this->attr, this->fileHandle);
		cout << leafNode->id << endl;
		leafNode->insert(key, rid);
		leafNode->serialize();
		root->insertKey(key, leafNode->id, INVALID_POINTER);
		root->serialize();
		return 0;
	}

// traverse till leaf node
	while (nodeType != LEAF) {
		AuxiloryNode* auxiloryNode = (AuxiloryNode*) node;
		auxiloryNode->search(key, node);
		visitedNodes.push(this->root);
		node->getNodeType(nodeType);
	}

//	try inserting
	LeafNode* leafNode = (LeafNode*) node;
	if (leafNode != 0 && leafNode->insert(key, rid) != -1) {
//		new key could fit nicely in existing thing, so do not need to do anything else
		leafNode->serialize();
		return 0;
	}
	if (leafNode == 0) {
		leafNode = new LeafNode(this->attr, this->fileHandle);
		leafNode->serialize();
	}

// for now , keep inserting in same node
	return -1;
}

AuxiloryNode::AuxiloryNode(const int &id, const Attribute &attr,
		const FileHandle &fileHandle) :
		Node(id, attr, fileHandle) {
	this->setNodeType(AUXILORY);
}

AuxiloryNode::AuxiloryNode(const int &id, const FileHandle & fileHandle) :
		Node(id, fileHandle) {
	this->setNodeType(AUXILORY);
}

RC AuxiloryNode::insertKey(const void* key, const int &leftChild,
		const int &rightChild) {
	int numberOfkeys;
	this->getNumberOfKeys(numberOfkeys);
	this->setNumberOfKeys(numberOfkeys + 1);
	char * cursor = (char *) this->data;
	cursor += this->getMetaDataSize();

	memcpy(cursor, &leftChild, sizeof(int));
	cursor += sizeof(int);

	memcpy(cursor, key, sizeof(int));
	cursor += sizeof(int);

	memcpy(cursor, &rightChild, sizeof(int));
	cursor += sizeof(int);

	return 0;
}

Node::Node(const int &id, const Attribute &attr, const FileHandle &fileHandle) {
	this->attr = attr;
	this->id = id;
	this->fileHandle = fileHandle;
	this->data = malloc(PAGE_SIZE);
	memset(this->data, 0, PAGE_SIZE);
}

Node::Node(const int &id, const FileHandle &fileHandle) {
	this->id = id;
	this->fileHandle = fileHandle;
	this->data = malloc(PAGE_SIZE);
	memset(this->data, 0, PAGE_SIZE);
}

RC Node::deserialize() {
	return this->fileHandle.readPage(this->id, this->data);
}

RC Node::serialize() {
	if (this->id == this->fileHandle.getNumberOfPages()) {
		return this->fileHandle.appendPage(this->data);
	}
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

RC AuxiloryNode::search(const void * key, Node*& nextNode) {
	char* cursor = (char*) this->data;
	cursor += this->getMetaDataSize();
	char * tempCursor;
	int numberOfKeys;
	this->getNumberOfKeys(numberOfKeys);
	if (this->attr.type == TypeReal || this->attr.type == TypeInt) {
		for (int i = 0; i < numberOfKeys; i++) {
			int nextPointer;
			int leftSubtreePointer = *((int*) cursor);
			cursor += sizeof(int);

			const void * firstKey = cursor;
			cursor += sizeof(int);

			int midSubtreePointer = *((int*) cursor);
			tempCursor = cursor;
			cursor += sizeof(int);

			const void * secondKey = cursor;
			cursor += sizeof(int);

			int rightSubtreePointer = *((int*) cursor);
			cursor += sizeof(int);

			if (key != NULL && firstKey != NULL && secondKey != NULL) {
				if (i == 0
						&& compare(key, firstKey, this->attr, false, false)
								>= 0) {
					nextPointer = leftSubtreePointer;
				} else if (i == numberOfKeys - 1
						&& compare(key, secondKey, this->attr, false, false)
								< 0) {
					nextPointer = rightSubtreePointer;
				}

				else if (compare(key, firstKey, this->attr, false, false) <= 0
						&& compare(key, secondKey, this->attr, false, false)
								> 0) {
					nextPointer = midSubtreePointer;
				} else {
					cursor = tempCursor;
					continue;
				}
			} else {
				if(key == NULL ) {
					nextPointer = leftSubtreePointer || midSubtreePointer || rightSubtreePointer;
				}
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
	return sizeof(int) * 3;
}

RC LeafNode::insert(const void* value, const RID &rid) {
	int numberOfKeys;
	char* cursor = (char*) this->data;
	this->getNumberOfKeys(numberOfKeys);
	cursor += this->getMetaDataSize();

	this->setNumberOfKeys(numberOfKeys + 1);
	return 0;
}

LeafNode::LeafNode(Attribute const &attr, FileHandle const& fileHandle) :
		Node(fileHandle.file->numberOfPages, attr, fileHandle) {
	this->setNodeType(LEAF);
}

LeafEntry::LeafEntry(void* data) {
	this->data = data;
}

RC LeafEntry::unparse(Attribute &attr, void* key, int& pageNum,int& slotNum){
	char * cursor = (char *) this->data;
	if(this->data == 0) {
		return -1;
	}
	if(attr.type == TypeInt) {
		memcpy(key, cursor, sizeof(int));
		cursor += sizeof(int);
		memcpy(&pageNum, cursor, sizeof(int));
		cursor += sizeof(int);
		memcpy(&slotNum, cursor, sizeof(int));
		cursor += sizeof(int);
		return 0;
	} else if (attr.type == TypeReal) {
		memcpy(key, cursor, sizeof(float));
		cursor += sizeof(float);
		memcpy(&pageNum, cursor, sizeof(int));
		cursor += sizeof(int);
		memcpy(&slotNum, cursor, sizeof(int));
		cursor += sizeof(int);
		return 0;
	} else if(attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(cursor);
		string keyString;
		varchar->unParse(keyString);
		varchar->data = 0;
		delete varchar;
		memcpy(key, keyString.c_str(), keyString.size());
		cursor += sizeof(int);
		cursor += keyString.size();
		memcpy(&pageNum, cursor, sizeof(int));
		cursor += sizeof(int);
		memcpy(&slotNum, cursor, sizeof(int));
		cursor += sizeof(int);
		return 0;
	}
	return -1;
}

LeafEntry* LeafEntry::parse(Attribute &attr, void* key, const int &pageNum,const int &slotNum) {
	void* leafEntryData = malloc(LeafEntry::getSize(attr, key, (int &)pageNum, (int&)slotNum));
	LeafEntry* leafEntry = new LeafEntry(leafEntryData);
	char * cursor = (char *) leafEntry->data;
	if(attr.type == TypeInt) {
		memcpy(cursor, key, sizeof(int));
		cursor += sizeof(int);
		memcpy(cursor, &pageNum, sizeof(int));
		cursor += sizeof(int);
		memcpy(cursor, &slotNum, sizeof(int));
		cursor += sizeof(int);
		return leafEntry;
	} else if (attr.type == TypeReal) {
		memcpy(key, cursor, sizeof(float));
		cursor += sizeof(float);
		memcpy(cursor, &pageNum, sizeof(int));
		cursor += sizeof(int);
		memcpy(cursor, &slotNum, sizeof(int));
		cursor += sizeof(int);
		return leafEntry;
	} else if(attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(key);
		string keyString;
		varchar->unParse(keyString);

		memcpy(cursor, varchar->data, sizeof(int) + keyString.size());
		cursor += sizeof(int) + keyString.size();
		delete varchar;

		memcpy(cursor, &pageNum, sizeof(int));
		cursor += sizeof(int);

		memcpy(cursor, &slotNum, sizeof(int));
		cursor += sizeof(int);
		return leafEntry;
		}
	return 0;
}

	int LeafEntry::getSize(Attribute &attr, void* key, int &pageNum, int &slotNum) {
		int size = 0;
		if(attr.type == TypeInt || attr.type == TypeReal) {
			size += sizeof(int);
		} else {
			VarcharParser* varchar = new VarcharParser(key);
			string keyString;
			varchar->unParse(keyString);
			size += sizeof(int) + keyString.size();
			delete varchar;
		}

		size += sizeof(int)*2;
		return size;
	}
