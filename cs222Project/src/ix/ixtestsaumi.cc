#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

void printNode(Node *node) {
	int numberOfKeys;
	node->getNumberOfKeys(numberOfKeys);
	char* cursor = (char*) node->data;
	cursor = cursor + node->getMetaDataSize();
	NodeType nodeType;
	node->getNodeType(nodeType);
	Entry *entry;
	int leftPointer = NULL, rightPointer = NULL;
	int tempKey;
	//cout << node->toJson() << endl;
	if (nodeType == AUXILORY) {
		entry = new AuxiloryEntry(cursor, node->attr);
		for (int i = 0; i < numberOfKeys; i++) {
			tempKey = *((int*) entry->getKey());

			void* key = malloc(node->attr.length);

			((AuxiloryEntry*) entry)->unparse(node->attr, key,
					rightPointer);

			entry = entry->getNextEntry();

			if (rightPointer != NULL) {
				Node *nextNode = new LeafNode(rightPointer, node->attr,
						node->fileHandle);
				nextNode->deserialize();
				printNode(nextNode);
			}
			if (leftPointer != NULL) {
				Node *nextNode = new LeafNode(leftPointer, node->attr,
						node->fileHandle);
				nextNode->deserialize();
				printNode(nextNode);
			}

			cout << endl;
		}

	}
}

int main() {
	remove("bulk_graph");
	Attribute attrs;
	attrs.length = 4;
	attrs.type = TypeInt;
	attrs.name = "id";
	IXFileHandle ifh;
	FileHandle fh;
	indexManager->createFile("bulk_graph");
	indexManager->openFile("bulk_graph", ifh);
	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;

	fh = *ifh.fileHandle;

	LeafNode * node = new LeafNode(attrs, fh);

	for (int i = 0; i < 10; i++) {
		LeafEntry * entry = LeafEntry::parse(attrs, &i, i + 1, i * 11);
		node->insertEntry(entry);
	}
	node->serialize();

	LeafNode * rightNode = new LeafNode(attrs, fh);

	for (int i = 15; i < 30; i++) {
		LeafEntry * entry = LeafEntry::parse(attrs, &i, i + 1, i + 2);
		rightNode->insertEntry(entry);
	}
	rightNode->serialize();

	Graph* graph = new Graph(fh);
	graph->deserialize();

	int key1 = 12;
	AuxiloryEntry* entry = AuxiloryEntry::parse(attrs, &key1,
			rightNode->id);
	graph->root->insertEntry(entry);
	graph->serialize();

	Node *node1;
	node1 = (Node*) graph->root;
	cout << ((AuxiloryNode*)node1)->toJson();
	int key =15;

	indexManager->deleteEntry(ifh,attrs,&key,rid);


}

