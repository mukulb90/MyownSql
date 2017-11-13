#include "ix.h"
#include <assert.h>

int main(int argc, char **argv) {

	Attribute attr;
	attr.type = TypeInt;
	attr.name = "Name";
	attr.length = 4;

	FileHandle fileHandle;

	AuxiloryNode* node = new AuxiloryNode(20, attr, fileHandle);

	int key1 = 20;
	AuxiloryEntry* entry =  AuxiloryEntry::parse(attr, &key1, 23, 45);
	node->insertEntry(entry);

	int key2 = 15;
	AuxiloryEntry* entry2 = AuxiloryEntry::parse(attr, &key2, 1, 24);
	node->insertEntry(entry2);

	int key3 = 16;
	AuxiloryEntry* entry3 = AuxiloryEntry::parse(attr, &key3, 24, 23);
	node->insertEntry(entry3);

	char* cursor = (char*) node->data;
	cursor += node->getMetaDataSize();

	int size = entry2->getEntrySize();
	int diff = memcmp(cursor, entry2->data, size);
	assert(diff == 0 && "Auxilory entry2 not inserted at correct position");

	cursor += entry2->getSpaceNeededToInsert();

	size = entry3->getEntrySize();
	diff = memcmp(cursor, entry3->data, size);
	assert(diff == 0 && "Auxilory entry3 not inserted at correct position");

	cursor += entry3->getSpaceNeededToInsert();

	size = entry->getEntrySize();
	diff = memcmp(cursor, entry->data, entry->getEntrySize());
	assert(diff == 0 && "Auxilory entry1 not inserted at correct position");

	cout << "Insertion test successful" << endl;


	return 0;

}
