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
    if(lowKeyInclusive) {
        ix_ScanIterator.lowOperator = GE_OP;
    } else {
        ix_ScanIterator.lowOperator = GT_OP;
    }

    if(highKeyInclusive) {
        ix_ScanIterator.highOperator = LE_OP;
    } else {
        ix_ScanIterator.highOperator = LT_OP;
    }

	ix_ScanIterator.attribute = attribute;
	FileHandle* fileHandle = ixfileHandle.fileHandle;

	Graph* graph = new Graph(*fileHandle, attribute);
	int rc = graph->deserialize();
	if (rc == -1) {
		delete graph;
		return rc;
	}


    if(lowKey == NULL) {
        ix_ScanIterator.lowKeyEntry = LeafEntry::parse(graph->attr, graph->getMinKey(), 0, 0);
    } else {
        ix_ScanIterator.lowKeyEntry = LeafEntry::parse(graph->attr, lowKey, 0, 0);
    }

    if(highKey == NULL) {
        ix_ScanIterator.highKeyEntry = LeafEntry::parse(graph->attr, graph->getMaxKey(), 0, 0);
    } else {
        ix_ScanIterator.highKeyEntry = LeafEntry::parse(graph->attr, highKey, 0, 0);
    }

	Node* node = graph->getRoot();
	Node* nextNode = 0;
	NodeType nodeType;
	node->getNodeType(nodeType);
	while (nodeType != LEAF) {
		AuxiloryNode* auxNode = (AuxiloryNode*) node;
		auxNode->search(lowKey, node);
		node->getNodeType(nodeType);
	}
    nextNode = node;
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
	Node* node = graph->getRoot();
	cout << ((AuxiloryNode*) node)->toJson() << endl;
	delete graph;
}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    int pageNum, slotNum;
    Entry* lowKeyEntry = this->lowKeyEntry;
    Entry* highKeyEntry = this->highKeyEntry;
    int numberOfKeys;
    this->leafNode->getNumberOfKeys(numberOfKeys);
    this->ixfileHandle->ixReadPageCounter++;
    CompOp operator1 = this->lowOperator;
    CompOp operator2 = this->highOperator;
    start:
        while(this->numberOfElementsIterated < numberOfKeys) {
            if(*(this->currentEntry) > *highKeyEntry) {
                return -1;
            }
            if(operator1 == GT_OP) {
                if(operator2 == LT_OP) {
//                < X <
                    if(*lowKeyEntry < *(this->currentEntry) && *(this->currentEntry) < *(highKeyEntry)) {
                        ((LeafEntry*)this->currentEntry)->unparse(this->attribute, key, pageNum, slotNum);
                        rid.pageNum = pageNum;
                        rid.slotNum = slotNum;
                        this->numberOfElementsIterated++;
                        this->currentEntry = this->currentEntry->getNextEntry();
                        return 0;
                    }
                } else {
//                < X <=
                    if(*lowKeyEntry < *(this->currentEntry) && *(this->currentEntry) <= *(highKeyEntry)) {
                        ((LeafEntry*)this->currentEntry)->unparse(this->attribute, key, pageNum, slotNum);
                        rid.pageNum = pageNum;
                        rid.slotNum = slotNum;
                        this->numberOfElementsIterated++;
                        this->currentEntry = this->currentEntry->getNextEntry();
                        return 0;
                    }
                }
            } else if(operator1 == GE_OP) {
                if(operator2 == LT_OP) {
//                <= X <
                    if(*lowKeyEntry <= *(this->currentEntry) && *(this->currentEntry) < *(highKeyEntry)) {
                        ((LeafEntry*)this->currentEntry)->unparse(this->attribute, key, pageNum, slotNum);
                        rid.pageNum = pageNum;
                        rid.slotNum = slotNum;
                        this->numberOfElementsIterated++;
                        this->currentEntry = this->currentEntry->getNextEntry();
                        return 0;
                    }
                } else {
//                <= X <=
                    if(*lowKeyEntry <= *(this->currentEntry) && *(this->currentEntry) <= *(highKeyEntry)) {
                        ((LeafEntry*)this->currentEntry)->unparse(this->attribute, key, pageNum, slotNum);
                        rid.pageNum = pageNum;
                        rid.slotNum = slotNum;
                        this->numberOfElementsIterated++;
                        this->currentEntry = this->currentEntry->getNextEntry();
                        return 0;
                    }
                }
            }
            this->currentEntry = currentEntry->getNextEntry();
            this->numberOfElementsIterated++;
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
	return 0;
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
	AuxiloryNode* internalRoot = new AuxiloryNode(0, fileHandle);
    internalRoot->serialize();
    internalRoot->setNumberOfKeys(0);
    AuxiloryNode* root = new AuxiloryNode(1, fileHandle);
    root->setNumberOfKeys(0);
    root->serialize();
    internalRoot->setLeftPointer(1);
	graph->internalRoot = internalRoot;
	return graph;
}

Graph::Graph(const FileHandle &fileHandle) {
	this->internalRoot = new AuxiloryNode(0, fileHandle);
	this->internalRoot->setLeftPointer(1);
	this->fileHandle = fileHandle;
    (new AuxiloryNode(1, fileHandle))->serialize();
}

Graph::Graph(const FileHandle &fileHandle, const Attribute& attr) {
	this->internalRoot = new AuxiloryNode(0, attr, fileHandle);
	this->attr = attr;
	this->fileHandle = fileHandle;
}

Graph::~Graph(){
	delete this->internalRoot;
}

RC Graph::serialize() {
	return this->internalRoot->serialize();
}


void* Graph::getMaxKey() {
    Node* node = this->getRoot();
    NodeType nodeType;
    node->getNodeType(nodeType);
    int numberOfEntries;
    int nextNodePointer;
    while(nodeType != LEAF) {
        node->getNumberOfKeys(numberOfEntries);
        Entry* entry = node->getFirstEntry();
        for (int i = 0; i < numberOfEntries-1; ++i) {
            entry = entry->getNextEntry();
        }
        nextNodePointer  = ((AuxiloryEntry*)entry)->getRightPointer();
        if(nextNodePointer == INVALID_POINTER) {
            nextNodePointer  = ((AuxiloryEntry*)entry)->getLeftPointer();
        }

        node = Node::getInstance(nextNodePointer, node->attr, node->fileHandle);
        node->getNodeType(nodeType);
    }
    node->getNumberOfKeys(numberOfEntries);
    Entry* entry = node->getFirstEntry();
    for (int i = 0; i < numberOfEntries-1; ++i) {
        entry = entry->getNextEntry();
    }
    return entry->data;
}

void* Graph::getMinKey() {
    Node* node = this->getRoot();
    NodeType nodeType;
    node->getNodeType(nodeType);
    int nextNodePointer;
    while(nodeType != LEAF) {
        Entry* entry = node->getFirstEntry();
        nextNodePointer  = ((AuxiloryEntry*)entry)->getLeftPointer();
        if(nextNodePointer == INVALID_POINTER) {
            nextNodePointer  = ((AuxiloryEntry*)entry)->getRightPointer();
        }

        node = Node::getInstance(nextNodePointer, node->attr, node->fileHandle);
        node->getNodeType(nodeType);
    }
    Entry* entry = node->getFirstEntry();
    return entry->data;
}

AuxiloryNode* Graph::getRoot() {
	int leftPointer;
	leftPointer = this->internalRoot->getLeftPointer();
	AuxiloryNode* root = new AuxiloryNode(leftPointer, this->attr, this->fileHandle);
    root->deserialize();
    return root;
}

void Graph::setRoot(AuxiloryNode *root) {
    this->internalRoot->setLeftPointer(root->id);
}

RC Graph::deserialize() {
	return this->internalRoot->deserialize();
}

RC Graph::insertEntry(const void * key, RID const& rid) {
	NodeType nodeType;
	Node* node = this->getRoot();
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
		AuxiloryEntry * auxEntry = AuxiloryEntry::parse(this->attr, key, leafNode->id);
        node->insertEntry(auxEntry);
        node->serialize();
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
				Node* secondNode;
				AuxiloryEntry* entryToBeInsertedInParent;
				NodeType nodeType;
				node->getNodeType(nodeType);
				if(nodeType == AUXILORY) {
					secondNode = new AuxiloryNode(node->attr, node->fileHandle);
					((AuxiloryNode*)node) -> split(secondNode, entryToBeInsertedInParent);
				} else {
					secondNode = new LeafNode(node->attr, node->fileHandle);
					((LeafNode*)node) -> split(secondNode, entryToBeInsertedInParent);
				}
//                TODO What if there is not enough space after split as well
                if(*entry >= *entryToBeInsertedInParent){
                    secondNode->insertEntry(entry);
                } else {
                    node->insertEntry(entry);
                }
				node->serialize();
				secondNode->serialize();
				if(visitedNodes.size() != 0) {
					node = visitedNodes.top();
					visitedNodes.pop();
				} else {
                    int pointerToLeftSubtree = node->id;
					node = new AuxiloryNode(node->attr, node->fileHandle);
					((AuxiloryNode*)node)->setLeftPointer(pointerToLeftSubtree);
                    node->insertEntry(entryToBeInsertedInParent);
                    node->serialize();
                    this->setRoot((AuxiloryNode*)node);
                    this->serialize();
                    goto cleanup;
				}
				entry = entryToBeInsertedInParent;
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
				leftSibling->serialize();
				node->serialize();

			} else {
				parentAuxiloryEntry->setLeftPointer(node->id);
				int rightNodeId = parentAuxiloryEntry->getRightPointer();
				LeafNode* rightSibling = new LeafNode(rightNodeId, this->attr, this->fileHandle);
				rightSibling->deserialize();
				rightSibling->addBefore((LeafNode*)node);
				rightSibling->serialize();
				node->serialize();
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
//		while(!visitedNodes.empty()){
//			Node* top = visitedNodes.top();
//			visitedNodes.pop();
//			NodeType nodeType;
//			top->getNodeType(nodeType);
//			if(this->getRoot() != top)
//				delete top;
//		}
//		while(!visitedEntries.empty()){
//			Entry* top = visitedEntries.top();
//			visitedEntries.pop();
//			delete top;
//		}

		return 0;
}


RC Graph::deleteKey(const void * key) {
	int rc = 0;
	NodeType nodeType;
	Node* node = this->getRoot();
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
        node->serialize();
		return rc;
	}
}

RC Node::deleteEntry(Entry * deleteEntry) {
	int rc = 0;
	int numberOfKeys = 0;
	int freeSpace = 0;
	this->getNumberOfKeys(numberOfKeys);
	if (numberOfKeys < 1) {
		rc = -1;
	}
	Entry *entry = this->getFirstEntry();

	for (int i = 0; i < numberOfKeys; i++) {
		int flag = 0;
		if (*entry == *deleteEntry) {
            int spaceShrinked = entry->getEntrySize();
            char* cursor = (char*)entry->data;
			entry = entry->getNextEntry();
			for (int j = i; j < numberOfKeys; j++) {
                int shiftSize = entry->getEntrySize();
				memcpy(cursor, entry->data, shiftSize);
                cursor += shiftSize;
				entry->data = (void*)((char*)entry->data+shiftSize);
			}
			this->getFreeSpace(freeSpace);
			this->setFreeSpace(freeSpace + spaceShrinked);
			this->setNumberOfKeys(numberOfKeys - 1);
            return 0;
		}
        entry = entry->getNextEntry();
    }

	return rc;
}

AuxiloryNode::AuxiloryNode(const Attribute &attr, const FileHandle &fileHandle) :
		Node(fileHandle.file->numberOfPages, attr, fileHandle) {
	this->setNodeType(AUXILORY);
	this->setLeftPointer(INVALID_POINTER);
}

AuxiloryNode::AuxiloryNode(const int &id, const Attribute &attr,
		const FileHandle &fileHandle) :
		Node(id, attr, fileHandle) {
	this->setNodeType(AUXILORY);
	this->setLeftPointer(INVALID_POINTER);
}

AuxiloryNode::AuxiloryNode(const int &id, const FileHandle & fileHandle) :
		Node(id, fileHandle) {
	this->setNodeType(AUXILORY);
	this->setLeftPointer(INVALID_POINTER);
}

AuxiloryNode::~AuxiloryNode() {

}

int AuxiloryNode::getLeftPointer() {
	int* leftPointer = (int*)malloc(sizeof(int));
	char * cursor = (char*) this->data;
	cursor += 3*sizeof(int);
	memcpy(leftPointer, cursor, sizeof(int));
	return *leftPointer;
}

int AuxiloryNode::setLeftPointer(const int &leftPointer) {
	char * cursor = (char*) this->data;
	cursor += 3*sizeof(int);
	memcpy(cursor, &leftPointer, sizeof(int));
	return 0;
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
	void* tempKey = malloc(this->attr.length+sizeof(int));
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
		((AuxiloryEntry*)firstEntry)->unparse(this->attr, tempKey, rightPointer);
		parentEntry = firstEntry;
		leftPointer = ((AuxiloryEntry*)firstEntry)->getLeftPointer();
        if(searchEntry->getKey() == NULL) {
            if(leftPointer != INVALID_POINTER) {
                nextPointer = leftPointer;
                break;
            } else {
                nextPointer = rightPointer;
                break;
            }
        }
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
	void* tempdata = malloc(this->attr.length+sizeof(int));
	int leftPointer, rightPointer;
	string jsonString = "{";
	jsonString += "\"keys\":[";
	char* cursor = (char*) this->data;
	cursor = cursor + this->getMetaDataSize();
	AuxiloryEntry *entry = new AuxiloryEntry(cursor, this->attr);
	int numberOfKeys;
	this->getNumberOfKeys(numberOfKeys);
	unordered_set<int> unique_children;
	leftPointer = this->getLeftPointer();
	vector<int> children;
	if(numberOfKeys > 0) {
		children.push_back(leftPointer);
		unique_children.insert(leftPointer);
	}
	for (int i = 0; i < numberOfKeys; i++) {
		jsonString += entry->toJson();
        if(i != numberOfKeys-1) {
            jsonString += ",";
        }
		entry->unparse(this->attr, tempdata, rightPointer);
		entry = (AuxiloryEntry*) entry->getNextEntry();
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
		// #HACK: Remove this Hacky Approach
			if (nodeType == LEAF || id == INVALID_POINTER) {
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
		int nodeEntrySize = nodeEntry->getEntrySize();

		if (*nodeEntry > *entry) {
			//			make space for new entry
			int shiftSize = PAGE_SIZE - startOffset - spaceRequiredByEntry;
			void* buffer = malloc(shiftSize);
			memcpy(buffer, cursor, shiftSize);
			memcpy(cursor, entry->data, spaceRequiredByEntry);
			cursor += spaceRequiredByEntry;
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

RC LeafNode::split(Node* secondNode, AuxiloryEntry* &entryToBeInsertedInParent) {
	int numberOfKeys = 0;
	this->getNumberOfKeys(numberOfKeys);
	Entry* entry = this->getFirstEntry();
	for (int i = 0; i<numberOfKeys; ++i) {
//		##TODO change this to capacity based spliting
		if(i<numberOfKeys/2) {
			entry = entry->getNextEntry();
		} else {
			secondNode->insertEntry(entry);
			this->deleteEntry(entry);
		}
	}

	void* key = secondNode->getFirstEntry()->data;
	AuxiloryEntry* parentEntry = AuxiloryEntry::parse(this->attr, key, secondNode->id);
	entryToBeInsertedInParent = parentEntry;
    this->addAfter((LeafNode*)secondNode);
	return 0;
}

RC AuxiloryNode::split(Node* secondNode, AuxiloryEntry* &entryToBeInsertedInParent) {
	int numberOfKeys;
	this->getNumberOfKeys(numberOfKeys);
	AuxiloryEntry* entry = (AuxiloryEntry*) this->getFirstEntry();
	AuxiloryNode* secondAuxiloryNode = (AuxiloryNode*)secondNode;
	bool is_first_redistribution = true;
	for(int i=0; i<numberOfKeys; i++) {
		if(i<numberOfKeys/2) {
			entry = (AuxiloryEntry*)entry->getNextEntry();
		} else {
			if(is_first_redistribution) {
				is_first_redistribution = false;
				secondAuxiloryNode->setLeftPointer(entry->getRightPointer());
				entryToBeInsertedInParent = AuxiloryEntry::parse(this->attr, entry->data, secondAuxiloryNode->id);
                this->deleteEntry(entry);
            } else {
				secondNode->insertEntry(entry);
				this->deleteEntry(entry);
			}
        }
	}
	return 0;
}

string LeafNode::toJson() {
    short leftPointer, rightPointer;
    this->getLeftSibling(leftPointer);
    this->getRightSibling(rightPointer);
	int numberOfKeys;
	this->getNumberOfKeys(numberOfKeys);
	string jsonString = "";
	jsonString = "{\"keys\":[";
	if(numberOfKeys > 0){
	    jsonString += "\"leftPointer:" + to_string(leftPointer) + "\", ";
	}
	char* cursor = (char*) this->data;
	cursor = cursor + this->getMetaDataSize();
	Entry *entry = new LeafEntry(cursor, this->attr);
	for (int i = 0; i < numberOfKeys; i++) {
		jsonString += entry->toJson();
		jsonString += " , ";
		entry = entry->getNextEntry();
	}

	if(numberOfKeys > 0){
	    jsonString += "\"rightPointer:" + to_string(rightPointer) +"\"";
	}
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
	return 0;
}

bool operator <(Entry& entry, Entry& entry2) {
	return compare(entry2.data, entry.data, entry.attr, false, false)
			< 0;
}

bool operator <=(Entry& entry, Entry& entry2) {
	return compare(entry2.data, entry.data, entry.attr, false, false)
			<= 0;
}
bool operator >(Entry & entry, Entry & entry2) {
	return compare(entry2.data, entry.data, entry.attr, false, false)
			> 0;
}

bool operator >=(Entry & entry, Entry & entry2) {
	return compare(entry2.data, entry.data, entry.attr, false, false)
			>= 0;
}

bool operator ==(Entry & entry, Entry & entry2) {
	return compare(entry2.data, entry.data, entry.attr, false, false)
			== 0;
}

LeafEntry::LeafEntry(void* data, Attribute &attribute) {
	this->data = data;
	this->attr = attribute;
}

int LeafEntry::getEntrySize() {
	return LeafEntry::getSize(this->attr, this->data);
}

int LeafEntry::getSpaceNeededToInsert() {
	return this->getEntrySize();
}

const void* LeafEntry::getKey() {
    if(this->attr.type == TypeVarChar) {
        string key;
        VarcharParser* varcharParser = new VarcharParser(this->data);
        varcharParser->unParse(key);
        return (const void *)key.c_str();
    }
	return this->data;
}

string LeafEntry::toJson() {
	void* key = malloc(this->attr.length+sizeof(int));
	int slotNum, pageNum;
	string json = "";
	json += "\"";
	this->unparse(this->attr, key, pageNum, slotNum);
	string keyString;
	if (attr.type == TypeInt) {
		keyString = to_string(*((int*) key));

	} else if (attr.type == TypeReal) {
		keyString = to_string(*((float*) key));
	} else if (attr.type == TypeVarChar) {
        string key;
        VarcharParser* vp = new VarcharParser(this->data);
        vp->unParse(keyString);
    }
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
		memcpy(key, keyString.c_str(), sizeof(int) + keyString.size());
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
        varchar->data = 0;
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

const void* AuxiloryEntry::getKey() {
	char * cursor = (char *)this->data;
	if(this->data == NULL) {
		return NULL;
	}
    if(this->attr.type == TypeVarChar) {
        string key;
        VarcharParser* varcharParser = new VarcharParser(this->data);
        varcharParser->unParse(key);
        return (void*)key.c_str();
    }
	return  cursor;
}

int AuxiloryEntry::getEntrySize() {
	return (new LeafEntry(this->data, this->attr))->getEntrySize() - sizeof(int);
}

AuxiloryEntry* AuxiloryEntry::parse(Attribute &attr, const void* key, const int &rightPointer) {
	void * data = malloc(AuxiloryEntry::getSize(attr, (void*)key));
	AuxiloryEntry* entry = new AuxiloryEntry(data, attr);
	char* cursor = (char*) entry->data;
	if(attr.type == TypeInt || attr.type == TypeReal) {
		memcpy(cursor, key, sizeof(int));
		cursor += sizeof(int);
	} else {
		VarcharParser* varchar = new VarcharParser((void*)key);
		string stringKey;
		varchar->unParse(stringKey);
		memcpy(cursor, key, sizeof(int) + stringKey.size());
		cursor += sizeof(int) + stringKey.size();
	}
	memcpy(cursor, &rightPointer, sizeof(int));
	return entry;
}

int AuxiloryEntry::getSize(Attribute &attr, void* key) {
	return LeafEntry::getSize(attr, key) - sizeof(int);
}

RC AuxiloryEntry::unparse(Attribute &attr, void* key,
		int &rightPointer) {
	char * cursor = (char*)this->data;
	if(attr.type == TypeInt || attr.type == TypeReal) {
		memcpy(key, cursor, sizeof(int));
		cursor += sizeof(int);
	} else {
		VarcharParser* varcharParser = new VarcharParser(cursor);
		string keyString;
		varcharParser->unParse(keyString);
		memcpy(key, cursor, sizeof(int) + keyString.size());
		cursor += sizeof(int) + keyString.size();
	}

	memcpy(&rightPointer, cursor, sizeof(int));
	return 0;
}

void AuxiloryEntry::setLeftPointer(int &leftPointer) {
	char * cursor = (char*)this->data;
	cursor -= sizeof(int);
	memcpy(cursor, &leftPointer, sizeof(int));
}

void AuxiloryEntry::setRightPointer(int &rightPointer) {
	char * cursor = (char*)this->data;
	if(attr.type == TypeInt || attr.type == TypeReal) {
		cursor += sizeof(int);
	} else {
		VarcharParser* varcharParser = new VarcharParser(cursor);
		string keyString;
		varcharParser->unParse(keyString);
		cursor += sizeof(int) + keyString.size();
	}
	memcpy(cursor, &rightPointer, sizeof(int));
}

int AuxiloryEntry::getLeftPointer() {
	int leftPointer;
	char * cursor = (char*)this->data;
	cursor -= sizeof(int);
	memcpy(&leftPointer, cursor, sizeof(int));
	return leftPointer;
}

int AuxiloryEntry::getRightPointer() {
	int rightPointer;
	char * cursor = (char*)this->data;
	if(attr.type == TypeInt || attr.type == TypeReal) {
		cursor += sizeof(int);
	} else {
		VarcharParser* varcharParser = new VarcharParser(cursor);
		string keyString;
		varcharParser->unParse(keyString);
		cursor += sizeof(int) + keyString.size();
	}
	memcpy(&rightPointer, cursor, sizeof(int));
	return rightPointer;
}

Entry* AuxiloryEntry::getNextEntry() {
	char* cursor = (char*) this->data;
	return (Entry*) new AuxiloryEntry(
			cursor + this->getEntrySize(), this->attr);
}

string AuxiloryEntry::toJson() {
	string jsonString = "";
	string keyString = "";
	if (attr.type == TypeInt) {
		keyString = to_string(*((int*) this->getKey()));

	} else if (attr.type == TypeReal) {
		keyString = to_string(*((float*) this->getKey()));
	} else if (attr.type == TypeVarChar) {
        string key;
        VarcharParser* vp = new VarcharParser(this->data);
        vp->unParse(key);
        keyString = "\"" + key + "\"";
	}
	jsonString = keyString;
	return jsonString;
}


int AuxiloryEntry::getSpaceNeededToInsert() {
	return this->getEntrySize();
}

