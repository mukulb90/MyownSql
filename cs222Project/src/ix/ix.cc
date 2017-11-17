#include "ix.h"

#include <string.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <stack>
#include <utility>
#include <unordered_set>

#include "../rbf/pfm.h"

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
		delete graph;
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
		delete graph;
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
	rc = graph->deleteKey(key);
	if (rc == 0) {
		ixfileHandle.ixWritePageCounter++;
	}
	graph->serialize();
	delete graph;

	return rc;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
	if(ixfileHandle.fileHandle == NULL) {
		return -1;
	}
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
		delete graph;
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
	ix_ScanIterator.currentEntry = leafNode->getFirstEntry();
	delete graph;
	return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
		const Attribute &attribute) const {
	FileHandle fileHandle = *(ixfileHandle.fileHandle);
	Graph *graph = new Graph(fileHandle, attribute);
	int rc = graph->deserialize();
	Node* node = graph->root;
	cout << ((AuxiloryNode*) node)->toJson() << endl;
	delete graph;
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    int numberOfKeys;
    this->leafNode->getNumberOfKeys(numberOfKeys);
    start:
    this->ixfileHandle->ixReadPageCounter++;
	while (this->numberOfElementsIterated < numberOfKeys) {
		if (lowKey == NULL or highKey == NULL) {
			this->numberOfElementsIterated++;
			int pageNum, slotNum;
			((LeafEntry*)this->currentEntry)->unparse(this->attribute, key, pageNum, slotNum);
			rid.pageNum = pageNum;
			rid.slotNum = slotNum;
			this->currentEntry = this->currentEntry->getNextEntry();
			return 0;
//		} else if (this->lowKeyInclusive && this->highKeyInclusive) {
//			if (compare(this->lowKey, cursor, this->attribute, false, false)
//					<= 0
//					&& compare(this->highKey, cursor, this->attribute, false,
//							false) >= 0) {
//				this->numberOfElementsIterated++;
//				this->getRIDAndKey((char*) cursor, rid, key);
//				return 0;
//			}
//		} else if (this->lowKeyInclusive && !this->highKeyInclusive) {
//			if (compare(this->lowKey, cursor, this->attribute, false, false)
//					<= 0
//					&& compare(this->highKey, cursor, this->attribute, false,
//							false) > 0) {
//				this->numberOfElementsIterated++;
//				this->getRIDAndKey((char*) cursor, rid, key);
//				return 0;
//			}
//		} else if (!this->lowKeyInclusive && this->highKeyInclusive) {
//			if (compare(this->lowKey, cursor, this->attribute, false, false) < 0
//					&& compare(this->highKey, cursor, this->attribute, false,
//							false) >= 0) {
//				this->numberOfElementsIterated++;
//				this->getRIDAndKey((char*) cursor, rid, key);
//				return 0;
//			} else if (!this->lowKeyInclusive && !this->highKeyInclusive) {
//				if (compare(this->lowKey, cursor, this->attribute, false, false)
//						< 0
//						&& compare(this->highKey, cursor, this->attribute,
//								false, false) > 0) {
//					this->numberOfElementsIterated++;
//					this->getRIDAndKey((char*) cursor, rid, key);
//					return 0;
//				}
//			}
		}
	}
    if(this->numberOfElementsIterated == numberOfKeys) {
        short rightSibling;
        this->leafNode->getRightSibling(rightSibling);
        if(rightSibling == INVALID_POINTER) {
            return -1;
        }
        this->leafNode = (LeafNode*)Node::getInstance(rightSibling, this->attribute, this->leafNode->fileHandle);
        this->currentEntry = this->leafNode->getFirstEntry();
        this->numberOfElementsIterated = 0;
        goto start;
    }
	return -1;
}

RC IX_ScanIterator::close() {
	if (this->ixfileHandle->fileHandle->file == 0) {
		return -1;
	}
	return PagedFileManager::instance()->closeFile(
			*this->ixfileHandle->fileHandle);
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

Graph::~Graph(){
	delete this->root;
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
	Entry* parentEntry = 0;
	stack<Node*> visitedNodes;
	stack<Entry*> visitedEntries;
	node->getNodeType(nodeType);
	Entry* entry = LeafEntry::parse(this->attr, (void*) key, rid.pageNum,
			rid.slotNum);

	int numberOfKeys;
	node->getNumberOfKeys(numberOfKeys);
	if (numberOfKeys == 0) {
//		Initialize graph with one root and one leaf Node
		LeafNode* leafNode = new LeafNode(this->attr, this->fileHandle);
		leafNode->insertEntry(entry);
		leafNode->serialize();
		AuxiloryEntry * entry = AuxiloryEntry::parse(this->attr, key,
				INVALID_POINTER, leafNode->id);
		root->insertEntry(entry);
		root->serialize();
		goto cleanup;
	}

// traverse till leaf node
	while (nodeType != LEAF) {
		AuxiloryNode* auxiloryNode = (AuxiloryNode*) node;
        visitedNodes.push(node);
        visitedEntries.push(parentEntry);
		parentEntry = auxiloryNode->search(key, node);
		if (node == NULL) {
			break;
		}
		node->getNodeType(nodeType);
	}

	while(entry != NULL) {

	//		try inserting
	//		LeafNode* leafNode = (LeafNode*) node;
		if(node != NULL) {
			if ( node->insertEntry(entry) != -1) {
		//		new key could fit nicely in existing thing, so do not need to do anything else
				node->serialize();
				goto cleanup;
			} else {
				LeafNode* firstNode = new LeafNode(node->attr, node->fileHandle);
				AuxiloryEntry* entryPointingToBothNodes;
				((LeafNode*)node)->split((Node*)firstNode, entryPointingToBothNodes);
//                TODO What if there is not enough space after split as well
                if(*entryPointingToBothNodes < *entry){
                    firstNode->insertEntry(entry);
                } else {
                    node->insertEntry(entry);
                }
				node->serialize();
				firstNode->serialize();
				node = visitedNodes.top();
				visitedNodes.pop();
				entry = entryPointingToBothNodes;
				continue;
			}
		}

		if (node == NULL) {
			node = new LeafNode(this->attr, this->fileHandle);
			node->insertEntry(entry);
			AuxiloryEntry* parentAuxiloryEntry = (AuxiloryEntry*) parentEntry;
			if (*entry >= *parentEntry) {
				parentAuxiloryEntry->setRightPointer(node->id);

				int leftSiblingId = parentAuxiloryEntry->getLeftPointer();
				LeafNode* leftSibling = new LeafNode(leftSiblingId, this->attr,
						this->fileHandle);
				leftSibling->deserialize();
				leftSibling->addAfter((LeafNode*)node);

			} else {
				parentAuxiloryEntry->setLeftPointer(node->id);
				int rightNodeId = parentAuxiloryEntry->getRightPointer();
				LeafNode* rightSibling = new LeafNode(rightNodeId, this->attr, this->fileHandle);
				rightSibling->deserialize();
				rightSibling->addBefore((LeafNode*)node);

			}
			Node* parent = visitedNodes.top();
			visitedNodes.pop();
			parent->serialize();

//			Not needed as node will be saved by addAfter and addBefore
//			node->serialize();
			goto cleanup;
		}
	}

// for now , keep inserting in same node
	return -1;

	cleanup:
		while(!visitedNodes.empty()){
			Node* top = visitedNodes.top();
			visitedNodes.pop();
			NodeType nodeType;
			top->getNodeType(nodeType);
			if(this->root != top)
				delete top;
		}
		while(!visitedEntries.empty()){
			Entry* top = visitedEntries.top();
			visitedEntries.pop();
			delete top;
		}

		return 0;
}


RC Graph::deleteKey(const void * key) {
	int rc = 0;
	NodeType nodeType;
	Node* node = this->root;
	Entry* parentEntry = 0;
	node->getNodeType(nodeType);

	while (nodeType != LEAF) {
		AuxiloryNode* auxiloryNode = (AuxiloryNode*) node;
		parentEntry = auxiloryNode->search(key, node);
		if (node == NULL) {
			cout << "Entry could not be found" << endl;
			rc = -1;
		}
		node->getNodeType(nodeType);
		LeafEntry *deleteEntry = new LeafEntry((void*) key, this->attr);
		rc = ((LeafNode*) node)->deleteEntry(deleteEntry);
		(LeafNode*) node->serialize();
		return rc;
	}
}

RC LeafNode::deleteEntry(Entry * deleteEntry) {
	int rc = 0;
	int numberOfKeys = 0;
	int freeSpace = 0;
	this->getNumberOfKeys(numberOfKeys);
	if (numberOfKeys < 1) {
		rc = -1;
	}
	char* cursor = (char*) this->data;
	cursor = cursor + this->getMetaDataSize();
	Entry *entry = new LeafEntry(cursor, this->attr);

	for (int i = 0; i < numberOfKeys; i++) {
		int flag = 0;
		if (*entry == *deleteEntry) {
			entry = entry->getNextEntry();
			for (int j = i; j < numberOfKeys; j++) {
				memcpy((char*) entry->data - deleteEntry->getEntrySize(),
						entry->data, entry->getEntrySize());
				entry = entry->getNextEntry();
			}
			flag = 1;
			this->getFreeSpace(freeSpace);
			this->setFreeSpace(freeSpace + deleteEntry->getEntrySize());
			this->setNumberOfKeys(numberOfKeys - 1);
		}
		entry = entry->getNextEntry();
		if (flag == 1) {
			break;
		}
	}

	return rc;
}

AuxiloryNode::AuxiloryNode(const Attribute &attr, const FileHandle &fileHandle) :
		Node(fileHandle.file->numberOfPages, attr, fileHandle) {
	this->setNodeType(AUXILORY);
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

AuxiloryNode::~AuxiloryNode() {

}

Node::Node(const int &id, const Attribute &attr, const FileHandle &fileHandle) {
	this->attr = attr;
	this->id = id;
	this->fileHandle = fileHandle;
	this->data = malloc(PAGE_SIZE);
	memset(this->data, 0, PAGE_SIZE);
	this->setFreeSpace(PAGE_SIZE - this->getMetaDataSize());
}

Node::Node(const int &id, const FileHandle &fileHandle) {
	this->id = id;
	this->fileHandle = fileHandle;
	this->data = malloc(PAGE_SIZE);
	memset(this->data, 0, PAGE_SIZE);
	this->setFreeSpace(PAGE_SIZE - this->getMetaDataSize());
}

Node::~Node() {
	freeIfNotNull(this->data);
}

Node* Node::getInstance(const int &id, const Attribute &attr, const FileHandle &fileHandle) {
    Node* node = new Node(id, attr, fileHandle);
    node->deserialize();
    NodeType nodeType;
    node->getNodeType(nodeType);
    if(nodeType == AUXILORY) {
        return (AuxiloryNode *) node;
    } else {
        return (LeafNode*)node;
    }
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

RC Node::getFreeSpace(int &freeSpace) {
	char* cursor = (char*) this->data;
	cursor += 2 * sizeof(int);
	memcpy(&freeSpace, cursor, sizeof(int));
	return 0;
}

RC Node::setFreeSpace(const int &freeSpace) {
	char* cursor = (char*) this->data;
	cursor += 2 * sizeof(int);
	memcpy(cursor, &freeSpace, sizeof(int));
	return 0;
}

Entry* AuxiloryNode::search(const void * key, Node* &nextNode) {
	char* cursor = (char*) this->data;
	cursor += this->getMetaDataSize();
	void* tempKey = malloc(this->attr.length);
	Entry* parentEntry = 0;
	int numberOfKeys, nextPointer = INVALID_POINTER, leftPointer, rightPointer;
	this->getNumberOfKeys(numberOfKeys);
	if (numberOfKeys == 0) {
		nextNode = NULL;
		return parentEntry;
	}
	Entry* searchEntry = new AuxiloryEntry((void*) key, this->attr);
	Entry* firstEntry = new AuxiloryEntry(cursor, this->attr);

	for (int i = 0; i < numberOfKeys; i++) {
		((AuxiloryEntry*)firstEntry)->unparse(this->attr, tempKey, leftPointer, rightPointer);
		parentEntry = firstEntry;
		if (*searchEntry
				< *firstEntry && leftPointer != INVALID_POINTER) {
			nextPointer = leftPointer;
			break;
		}
		if (i == numberOfKeys - 1) {
			if (*searchEntry >= *firstEntry && rightPointer != INVALID_POINTER) {
				nextPointer = rightPointer;
				break;
			}
		}
		firstEntry = (AuxiloryEntry*) firstEntry->getNextEntry();
	}
	freeIfNotNull(tempKey);
	if (nextPointer == INVALID_POINTER) {
		nextNode = NULL;
		return parentEntry;
	}
	nextNode = new AuxiloryNode(nextPointer, this->attr, this->fileHandle);
	nextNode->deserialize();
	return parentEntry;
}

string AuxiloryNode::toJson() {

	void* tempdata = malloc(this->attr.length);
	int leftPointer, rightPointer;
	string jsonString = "{";
	jsonString += "\"keys\":[";
	char* cursor = (char*) this->data;
	cursor = cursor + this->getMetaDataSize();
	AuxiloryEntry *entry = new AuxiloryEntry(cursor, this->attr);
	int numberOfKeys;
	this->getNumberOfKeys(numberOfKeys);
	unordered_set<int> unique_children;

	vector<int> children;
	for (int i = 0; i < numberOfKeys; i++) {
		jsonString += entry->toJson();
        if(i != numberOfKeys-1) {
            jsonString += ",";
        }
		entry->unparse(this->attr, tempdata, leftPointer, rightPointer);
		entry = (AuxiloryEntry*) entry->getNextEntry();
		if (unique_children.find(leftPointer) == unique_children.end()) {
			children.push_back(leftPointer);
			unique_children.insert(leftPointer);
		}
		if (unique_children.find(rightPointer) == unique_children.end()) {
			children.push_back(rightPointer);
			unique_children.insert(rightPointer);
		}

	}
	jsonString += "],";
	jsonString += "\"children\":[";
	int count = 0;
	int size = children.size();
	for (vector<int>::iterator it = children.begin(); it != children.end();
			++it) {
		int id = *it;
		Node *childNode = Node::getInstance(id, this->attr, this->fileHandle);
		NodeType nodeType;
		childNode->getNodeType(nodeType);
		if (nodeType == LEAF) {
			jsonString += ((LeafNode*) childNode)->toJson();
		} else {
			jsonString += ((AuxiloryNode*) childNode)->toJson();
		}

		if (count != size - 1) {
			jsonString += ",";

		}
		count++;

	}
	jsonString += "]}";
	free(tempdata);
	return jsonString;

}

RC Node::getMetaDataSize() {
	return sizeof(int) * 4;
}

Entry* Node::getFirstEntry() {
	char* cursor = (char*) this->data;
	cursor += this->getMetaDataSize();
	NodeType nodeType;
	this->getNodeType(nodeType);
	if(nodeType == LEAF) {
		return new LeafEntry(cursor, this->attr);
	}
	return new AuxiloryEntry(cursor, this->attr);
}

RC Node::insertEntry(Entry* entry) {
	int numberOfKeys, freeSpace, spaceRequiredByEntry;
	char* cursor = (char*) this->data;
	int startOffset = 0;
	this->getNumberOfKeys(numberOfKeys);
	this->getFreeSpace(freeSpace);

	spaceRequiredByEntry = entry->getEntrySize();
	if (spaceRequiredByEntry > freeSpace) {
		//		Not enough Space in the Leaf Node
		return -1;
	}

	cursor += this->getMetaDataSize();
	startOffset += this->getMetaDataSize();
	Attribute attr;
	Entry* nodeEntry;

	//	Find correct position to insert such that Entries are sorted
	for (int i = 0; i < numberOfKeys; i++) {
		NodeType nodeType;
		this->getNodeType(nodeType);
		if (nodeType == AUXILORY) {
			nodeEntry = new AuxiloryEntry(cursor, this->attr);
		} else {
			nodeEntry = new LeafEntry(cursor, this->attr);
		}
		int nodeEntrySize = nodeEntry->getSpaceNeededToInsert();

		if (*nodeEntry > *entry) {
			//			make space for new entry
			int shiftSize = PAGE_SIZE - startOffset - spaceRequiredByEntry;
			void* buffer = malloc(shiftSize);
			memcpy(buffer, cursor, shiftSize);
			memcpy(cursor, entry->data, spaceRequiredByEntry);
			cursor += nodeEntrySize;
			memcpy(cursor, buffer, shiftSize);
			this->setNumberOfKeys(numberOfKeys + 1);
			this->setFreeSpace(freeSpace - spaceRequiredByEntry);
			free(buffer);
			return 0;
		} else {
			cursor += nodeEntrySize;
			startOffset += nodeEntrySize;
		}
	}
	memcpy(cursor, entry->data, spaceRequiredByEntry);
	this->setNumberOfKeys(numberOfKeys + 1);
	this->setFreeSpace(freeSpace - spaceRequiredByEntry);
	return 0;
}

LeafNode::LeafNode(Attribute const &attr, FileHandle const& fileHandle) :
		Node(fileHandle.file->numberOfPages, attr, fileHandle) {
	this->setNodeType(LEAF);
	this->setRightSibling(-1);
	this->setLeftSibling(-1);
}

LeafNode::LeafNode(const int &id, Attribute const &attr,
		FileHandle const& fileHandle) :
		Node(id, attr, fileHandle) {
	this->setNodeType(LEAF);
	this->setLeftSibling(-1);
	this->setRightSibling(-1);
}

LeafNode::~LeafNode() {

}

RC LeafNode::setLeftSibling(const short &pageNum) {
	char* cursor = (char*) this->data;
	cursor += 3 * sizeof(int);
	memcpy(cursor, &pageNum, sizeof(short));
	return 0;
}

RC LeafNode::getLeftSibling( short &pageNum) {
	char* cursor = (char*) this->data;
	cursor += 3 * sizeof(int);
	memcpy(&pageNum, cursor, sizeof( short));
	return 0;
}

RC LeafNode::setRightSibling(const short &pageNum) {
	char* cursor = (char*) this->data;
	cursor += 3*sizeof(int) + sizeof(short);
	memcpy(cursor, &pageNum, sizeof(short));
	return 0;
}

RC LeafNode::getRightSibling(short &pageNum) {
	char* cursor = (char*) this->data;
	cursor += 3*sizeof(int) + sizeof(short);
	memcpy(&pageNum, cursor, sizeof(short));
	return 0;
}

RC LeafNode::split(Node* firstNode, AuxiloryEntry* &entryPointingToBothNodes) {
	int numberOfKeys = 0;
	this->getNumberOfKeys(numberOfKeys);
	Entry* entry = this->getFirstEntry();
	for (int i = 0; i<numberOfKeys; ++i) {
		if(i<numberOfKeys/2) {
			firstNode->insertEntry(entry);
			this->deleteEntry(entry);
		} else {
			break;
		}
	}

	void* key = this->getFirstEntry()->getKey();
	AuxiloryEntry* parentEntry = AuxiloryEntry::parse(this->attr, key, firstNode->id, this->id);
	entryPointingToBothNodes = parentEntry;
    this->addBefore((LeafNode*)firstNode);
	return 0;
}

string LeafNode::toJson() {
    short leftPointer, rightPointer;
    this->getLeftSibling(leftPointer);
    this->getRightSibling(rightPointer);
	string jsonString = "";
	jsonString = "{\"keys\":[";
    jsonString += "\"leftPointer:" + to_string(leftPointer) + "\", ";
	char* cursor = (char*) this->data;
	cursor = cursor + this->getMetaDataSize();
	int numberOfKeys;
	this->getNumberOfKeys(numberOfKeys);
	Entry *entry = new LeafEntry(cursor, this->attr);
	for (int i = 0; i < numberOfKeys; i++) {
		jsonString += entry->toJson();
		i != numberOfKeys - 1 ? jsonString += " , " : "";
		entry = entry->getNextEntry();
	}

    jsonString += ", \"rightPointer:" + to_string(rightPointer) +"\"";
    jsonString += "]}";
	return jsonString;

}

RC LeafNode::addAfter(LeafNode* node) {
    short temp;
	this->getRightSibling(temp);
	this->setRightSibling((short)node->id);
	node->setRightSibling(temp);
	LeafNode* thirdNode = new LeafNode(temp, this->attr, this->fileHandle);
	thirdNode->deserialize();
	thirdNode->setLeftSibling((short)node->id);
	node->setLeftSibling((short)this->id);
	this->serialize();
	node->serialize();
	thirdNode->serialize();
	return 0;
}

RC LeafNode::addBefore(LeafNode* node) {
    short temp;
	this->getLeftSibling(temp);
	this->setLeftSibling((short)node->id);
	node->setLeftSibling(temp);
	LeafNode* previousNode = new LeafNode(temp, this->attr, this->fileHandle);
	previousNode->deserialize();
	previousNode->setRightSibling((short)node->id);
	node->setRightSibling((short)this->id);
	previousNode->serialize();
	node->serialize();
	this->serialize();
	return 0;
}

bool operator <(Entry& entry, Entry& entry2) {
	return compare(entry2.getKey(), entry.getKey(), entry.attr, false, false)
			< 0;
}

bool operator <=(Entry& entry, Entry& entry2) {
	return compare(entry2.getKey(), entry.getKey(), entry.attr, false, false)
			<= 0;
}
bool operator >(Entry & entry, Entry & entry2) {
	return compare(entry2.getKey(), entry.getKey(), entry.attr, false, false)
			> 0;
}

bool operator >=(Entry & entry, Entry & entry2) {
	return compare(entry2.getKey(), entry.getKey(), entry.attr, false, false)
			>= 0;
}

bool operator ==(Entry & entry, Entry & entry2) {
	return compare(entry2.getKey(), entry.getKey(), entry.attr, false, false)
			== 0;
}

LeafEntry::LeafEntry(void* data, Attribute &attribute) {
	this->data = data;
	this->attr = attribute;
}

int LeafEntry::getEntrySize() {
	return LeafEntry::getSize(this->attr, this->getKey());
}

int LeafEntry::getSpaceNeededToInsert() {
	return this->getEntrySize();
}

void* LeafEntry::getKey() {
	return this->data;
}

string LeafEntry::toJson() {
	void* key = malloc(this->attr.length);
	int slotNum, pageNum;
	string json = "";
	json += "\"";
	this->unparse(this->attr, key, pageNum, slotNum);
	string keyString = "";
	if (attr.type == TypeInt || attr.type == TypeReal) {
		keyString = to_string(*((int*) key));

	} else if (attr.type == TypeReal) {
		keyString = to_string(*((float*) key));
	} else if (attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(this->getKey());
		varchar->unParse(keyString);
		varchar->data = 0;
	}
	keyString = to_string(*((int*) key));
	json += keyString + ":";
	json += "(" + to_string(pageNum) + "," + to_string(slotNum) + ")";
	json += "\"";
	return json;
}

RC LeafEntry::unparse(Attribute &attr, void* key, int& pageNum, int& slotNum) {
	char * cursor = (char *) this->data;
	if (this->data == 0) {
		return -1;
	}
	if (attr.type == TypeInt) {
		memcpy(key, cursor, sizeof(int));
		cursor += sizeof(int);
		memcpy(&pageNum, cursor, sizeof(int));
		cursor += sizeof(int);
		memcpy(&slotNum, cursor, sizeof(int));
		return 0;
	} else if (attr.type == TypeReal) {
		memcpy(key, cursor, sizeof(float));
		cursor += sizeof(float);
		memcpy(&pageNum, cursor, sizeof(int));
		cursor += sizeof(int);
		memcpy(&slotNum, cursor, sizeof(int));
		return 0;
	} else if (attr.type == TypeVarChar) {
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
		return 0;
	}
	return -1;
}

LeafEntry* LeafEntry::parse(Attribute &attr, const void* key,
		const int &pageNum, const int &slotNum) {
	void* leafEntryData = malloc(LeafEntry::getSize(attr, key));
	LeafEntry* leafEntry = new LeafEntry(leafEntryData, attr);
	char * cursor = (char *) leafEntry->data;
	if (attr.type == TypeInt) {
		memcpy(cursor, key, sizeof(int));
		cursor += sizeof(int);
		memcpy(cursor, &pageNum, sizeof(int));
		cursor += sizeof(int);
		memcpy(cursor, &slotNum, sizeof(int));
		return leafEntry;
	} else if (attr.type == TypeReal) {
		memcpy(cursor, key, sizeof(float));
		cursor += sizeof(float);
		memcpy(cursor, &pageNum, sizeof(int));
		cursor += sizeof(int);
		memcpy(cursor, &slotNum, sizeof(int));
		return leafEntry;
	} else if (attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser((void*) key);
		string keyString;
		varchar->unParse(keyString);

		memcpy(cursor, varchar->data, sizeof(int) + keyString.size());
		cursor += sizeof(int) + keyString.size();
		delete varchar;

		memcpy(cursor, &pageNum, sizeof(int));
		cursor += sizeof(int);

		memcpy(cursor, &slotNum, sizeof(int));
		return leafEntry;
	}
	return 0;
}

void LeafEntry::setPageNum(int &pageNum) {
	char* cursor = (char*) this->data;
	if (attr.type == TypeInt) {
		cursor += sizeof(int);
		memcpy(cursor, &pageNum, sizeof(int));
		return;
	} else if (attr.type == TypeReal) {
		cursor += sizeof(float);
		memcpy(cursor, &pageNum, sizeof(int));
		return;
	} else if (attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(cursor);
		string keyString;
		varchar->unParse(keyString);
		cursor += sizeof(int) + keyString.size();
		delete varchar;
		memcpy(cursor, &pageNum, sizeof(int));
		return;
	}
}

void LeafEntry::setSlotNum(int &slotNum) {
	char* cursor = (char*) this->data;
	if (attr.type == TypeInt) {
		cursor += 2 * sizeof(int);
		memcpy(cursor, &slotNum, sizeof(int));
		return;
	} else if (attr.type == TypeReal) {
		cursor += sizeof(float) + sizeof(int);
		memcpy(cursor, &slotNum, sizeof(int));
		return;
	} else if (attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(cursor);
		string keyString;
		varchar->unParse(keyString);
		cursor += sizeof(int) + keyString.size();
		cursor += sizeof(int);
		delete varchar;
		memcpy(cursor, &slotNum, sizeof(int));
		return;
	}
}

int LeafEntry::getPageNum() {
	int pageNum;
	char* cursor = (char*) this->data;
	if (attr.type == TypeInt) {
		cursor += sizeof(int);
		memcpy(&pageNum, cursor, sizeof(int));
		return pageNum;
	} else if (attr.type == TypeReal) {
		cursor += sizeof(float);
		memcpy(&pageNum, cursor, sizeof(int));
		return pageNum;
	} else if (attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(cursor);
		string keyString;
		varchar->unParse(keyString);
		cursor += sizeof(int) + keyString.size();
		delete varchar;
		memcpy(&pageNum, cursor, sizeof(int));
		return pageNum;
	}
	return INVALID_POINTER;
}

int LeafEntry::getSlotNum() {
	char* cursor = (char*) this->data;
	int slotNum;
	if (attr.type == TypeInt) {
		cursor += 2 * sizeof(int);
		memcpy(&slotNum, cursor, sizeof(int));
		return slotNum;
	} else if (attr.type == TypeReal) {
		cursor += sizeof(float) + sizeof(int);
		memcpy(&slotNum, cursor, sizeof(int));
		return slotNum;
	} else if (attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(cursor);
		string keyString;
		varchar->unParse(keyString);
		cursor += sizeof(int) + keyString.size();
		cursor += sizeof(int);
		delete varchar;
		memcpy(&slotNum, cursor, sizeof(int));
		return slotNum;
	}
	return INVALID_POINTER;
}

int LeafEntry::getSize(Attribute &attr, const void* key) {
	int size = 0;
	if (attr.type == TypeInt || attr.type == TypeReal) {
		size += sizeof(int);
	} else {
		VarcharParser* varchar = new VarcharParser((void*) key);
		string keyString;
		varchar->unParse(keyString);
		size += sizeof(int) + keyString.size();
		delete varchar;
	}

	size += sizeof(int) * 2;
	return size;
}

Entry* LeafEntry::getNextEntry() {
	char* cursor = (char*) this->data;
	return (Entry*) new LeafEntry(cursor + this->getEntrySize(), this->attr);
}

AuxiloryEntry::AuxiloryEntry(void* data, Attribute & attr) {
	this->data = data;
	this->attr = attr;
}

void* AuxiloryEntry::getKey() {
	char * cursor = (char *)this->data;
	if(this->data == NULL) {
		return NULL;
	}
	cursor += sizeof(int);
	return  cursor;
}

int AuxiloryEntry::getEntrySize() {
	return (new LeafEntry(this->data, this->attr))->getEntrySize();
}

AuxiloryEntry* AuxiloryEntry::parse(Attribute &attr, const void* key,
		const int &leftPointer, const int &rightPointer) {
	void * data = malloc(AuxiloryEntry::getSize(attr, (void*)key));
	AuxiloryEntry* entry = new AuxiloryEntry(data, attr);
	char* cursor = (char*) entry->data;
	memcpy(cursor, &leftPointer, sizeof(int));
	cursor += sizeof(int);
	if(attr.type == TypeInt || attr.type == TypeReal) {
		memcpy(cursor, key, sizeof(int));
		cursor += sizeof(int);
	} else {
		VarcharParser* varchar = new VarcharParser((void*)key);
		string stringKey;
		varchar->unParse(stringKey);
		memcpy(cursor, key, stringKey.size());
		cursor += stringKey.size();
	}
	memcpy(cursor, &rightPointer, sizeof(int));
	return entry;
}

int AuxiloryEntry::getSize(Attribute &attr, void* key) {
	return LeafEntry::getSize(attr, key);
}

RC AuxiloryEntry::unparse(Attribute &attr, void* key, int &leftPointer,
		int &rightPointer) {
	char * cursor = (char*)this->data;
	memcpy(&leftPointer, cursor, sizeof(int));
	cursor += sizeof(int);
	if(attr.type == TypeInt || attr.type == TypeReal) {
		memcpy(key, cursor, sizeof(int));
		cursor += sizeof(int);
	} else {
		VarcharParser* varcharParser = new VarcharParser(cursor);
		string keyString;
		varcharParser->unParse(keyString);
		memcpy(key, cursor, sizeof(keyString.size()));
		cursor += keyString.size();
	}

	memcpy(&rightPointer, cursor, sizeof(int));
	return 0;
}

void AuxiloryEntry::setLeftPointer(int &leftPointer) {
	char * cursor = (char*)this->data;
	memcpy(cursor, &leftPointer, sizeof(int));
}

void AuxiloryEntry::setRightPointer(int &rightPointer) {
	char * cursor = (char*)this->data;
	cursor += sizeof(int);
	if(attr.type == TypeInt || attr.type == TypeReal) {
		cursor += sizeof(int);
	} else {
		VarcharParser* varcharParser = new VarcharParser(cursor);
		string keyString;
		varcharParser->unParse(keyString);
		cursor += keyString.size();
	}
	memcpy(cursor, &rightPointer, sizeof(int));
}

int AuxiloryEntry::getLeftPointer() {
	int leftPointer;
	char * cursor = (char*)this->data;
	memcpy(&leftPointer, cursor, sizeof(int));
	return leftPointer;
}

int AuxiloryEntry::getRightPointer() {
	int rightPointer;
	char * cursor = (char*)this->data;
	cursor += sizeof(int);
	if(attr.type == TypeInt || attr.type == TypeReal) {
		cursor += sizeof(int);
	} else {
		VarcharParser* varcharParser = new VarcharParser(cursor);
		string keyString;
		varcharParser->unParse(keyString);
		cursor += keyString.size();
	}
	memcpy(&rightPointer, cursor, sizeof(int));
	return rightPointer;
}

Entry* AuxiloryEntry::getNextEntry() {
	char* cursor = (char*) this->data;
	return (Entry*) new AuxiloryEntry(
			cursor + this->getEntrySize() - sizeof(int), this->attr);
}

string AuxiloryEntry::toJson() {
	string jsonString = "";
	string keyString = "";
	if (attr.type == TypeInt || attr.type == TypeReal) {
		keyString = to_string(*((int*) this->getKey()));

	} else if (attr.type == TypeReal) {
		keyString = to_string(*((float*) this->getKey()));
	} else if (attr.type == TypeVarChar) {
		VarcharParser* varchar = new VarcharParser(this->getKey());
		varchar->unParse(keyString);
		varchar->data = 0;
	}
	jsonString = keyString;
	return jsonString;
}


int AuxiloryEntry::getSpaceNeededToInsert() {
	return this->getEntrySize() - sizeof(int);
}

