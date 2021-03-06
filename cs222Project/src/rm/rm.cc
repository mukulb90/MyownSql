
#include "rm.h"
#include <tuple>
#include <math.h>
#include <string.h>
#include <unordered_map>
#include <map>

#define TABLE_CATALOG_NAME "Tables"
#define COLUMNS_CATALOG_NAME "Columns"

//unordered_map<string, vector<Attribute>> cache;

void getTablesCatalogRecordDescriptor(vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.type = TypeInt;
	attr.name = "table-id";
	attr.length = 4;
	recordDescriptor.push_back(attr);

	Attribute attr2;
	attr2.type = TypeVarChar;
	attr2.name = "table-name";
	attr2.length = 50;
	recordDescriptor.push_back(attr2);

	Attribute attr3;
	attr3.type = TypeVarChar;
	attr3.name = "file-name";
	attr3.length = 50;
	recordDescriptor.push_back(attr3);

	Attribute attr4;
	attr4.type = TypeInt;
	attr4.name = "version";
	attr4.length = 4;
	recordDescriptor.push_back(attr4);

}

void getColumnsCatalogRecordDescriptor(vector<Attribute> &recordDescriptor) {

	Attribute attr;
	attr.type = TypeInt;
	attr.name = "table-id";
	attr.length = 4;
	recordDescriptor.push_back(attr);

	Attribute attr2;
	attr2.type = TypeVarChar;
	attr2.name = "column-name";
	attr2.length = 50;
	recordDescriptor.push_back(attr2);

	Attribute attr3;
	attr3.type = TypeInt;
	attr3.name = "column-type";
	attr3.length = 4;
	recordDescriptor.push_back(attr3);

	Attribute attr4;
	attr4.type = TypeInt;
	attr4.name = "column-length";
	attr4.length = 4;
	recordDescriptor.push_back(attr4);

	Attribute attr5;
	attr5.type = TypeInt;
	attr5.name = "column-position";
	attr5.length = 4;
	recordDescriptor.push_back(attr5);

	Attribute attr6;
	attr6.type = TypeInt;
	attr6.name = "version";
	attr6.length = 4;
	recordDescriptor.push_back(attr6);


	Attribute attr7;
	attr7.type = TypeInt;
	attr7.name = "isIndexCreated";
	attr7.length = 4;
	recordDescriptor.push_back(attr7);

}

void RelationManager::printTable(string tableName) {
	vector<Attribute> attrs;
	vector<string> projections;
	this->getAttributes(tableName, attrs);
	RM_ScanIterator *iter = new RM_ScanIterator();
	for (int i = 0; i < attrs.size(); ++i) {
		projections.push_back(attrs[i].name);
	}
	this->scan(tableName.c_str(), "", NO_OP, 0, projections, *iter);
	RID rid;
	void* record = malloc(PAGE_SIZE);
	while(iter->getNextTuple(rid, record) != -1) {
		this->printTuple(attrs, record);
	}
	freeIfNotNull(record);
	delete iter;
}

void* createTableData(vector<Attribute> recordDescriptor,
		vector<string> tableRecord) {
	void * data = malloc(1000);
	char * cursor = (char *) data;
	int numOfNullBytes = ceil(recordDescriptor.size() / 8.0);

	int nullBytes = 0;
	for(int i=0; i<numOfNullBytes; i++) {
		memcpy(cursor, &nullBytes, sizeof(char));
		cursor += 1;
	}

	for (int i = 0; i < recordDescriptor.size(); ++i) {
		Attribute attr = recordDescriptor[i];
		if (attr.type == TypeInt) {
			int value = stoi(tableRecord[i]);
			memcpy(cursor, &value, sizeof(int));
			cursor += sizeof(int);
		} else if (attr.type == TypeReal) {
			float value = stof(tableRecord[i]);
			memcpy(cursor, &value, sizeof(float));
			cursor += sizeof(float);
		} else if (attr.type == TypeVarChar) {
			string value = tableRecord[i];
			int length = value.size();
			const char * value_pointer = value.c_str();
			memcpy(cursor, &length, sizeof(int));
			cursor += sizeof(int);
			memcpy(cursor, value_pointer, length);
			cursor += length;
		}
	}
	return data;
}

vector<string> getTableData(vector<Attribute> recordDescriptor, void * data) {
	char * cursor = (char *) data;
	int numOfNullBytes = ceil(recordDescriptor.size() / 8.0);

	cursor += numOfNullBytes;

	vector<string> returnedData;
	for (int i = 0; i < recordDescriptor.size(); ++i) {
		Attribute attr = recordDescriptor[i];
		if (attr.type == TypeInt) {
			int value = *((int*)cursor);
			returnedData.push_back(to_string(value));
			cursor += sizeof(int);
		} else if (attr.type == TypeReal) {
			float value = *((float*)cursor);
			returnedData.push_back(to_string(value));
			cursor += sizeof(float);
		} else if (attr.type == TypeVarChar) {
			int length = *((int*)cursor);
			cursor += sizeof(int);
			returnedData.push_back(string(cursor, length));
			cursor += length;
		}
	}
	return returnedData;
}

int getNextTableId() {
	int max_so_far = 1;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> attrs;
	getTablesCatalogRecordDescriptor(attrs);
	vector<string> projectedAttributes;
	string queryName = attrs[0].name;
	for(int i=0; i< attrs.size(); i++) {
		projectedAttributes.push_back(attrs[i].name);
	}
	rbfm->openFile(TABLE_CATALOG_NAME, fileHandle);
    RBFM_ScanIterator* iterator = new RBFM_ScanIterator();
	rbfm->scan(fileHandle, attrs, queryName, NO_OP, 0, projectedAttributes, *iterator);
	int count = 0;
	RID rid;
	void* returned_data = malloc(1000);
	while(iterator->getNextRecord(rid, returned_data) != -1) {
		TableCatalogRecord* tcr = new TableCatalogRecord(returned_data);
		int id;
		string tableName;
		tcr->unParse(id, tableName);
		if(id > max_so_far) {
			max_so_far = id;
		}
		tcr->data = 0;
		delete tcr;
		count++;
	}
	freeIfNotNull(returned_data);
	delete iterator;
	rbfm->closeFile(fileHandle);
	return max_so_far+1;
}

TableCatalogRecord::TableCatalogRecord(void * data) {
	this->data = data;
}

TableCatalogRecord::~TableCatalogRecord() {
	freeIfNotNull(this->data);
}

TableCatalogRecord* TableCatalogRecord::parse(const int &id, const string &tableName, const int &version) {
	vector<Attribute> attrs;
	getTablesCatalogRecordDescriptor(attrs);
	vector<string> tableRecord;
	tableRecord.push_back(to_string(id));
	tableRecord.push_back(tableName);
	tableRecord.push_back(tableName);
	tableRecord.push_back(to_string(version));
	void* tableCatalogRecordData = createTableData(attrs, tableRecord);
	TableCatalogRecord* record = new TableCatalogRecord(tableCatalogRecordData);
	return record;
}

RC TableCatalogRecord::unParse(int &id, string &tableName) {
	vector<Attribute> tableAttr;
	getTablesCatalogRecordDescriptor(tableAttr);
	vector<string> tableRecord = getTableData(tableAttr, this->data);
	id = stoi(tableRecord[0]);
	tableName = tableRecord[1];
	return 0;
}

ColumnsCatalogRecord::ColumnsCatalogRecord(void * data) {
	this->data = data;
}

ColumnsCatalogRecord::~ColumnsCatalogRecord() {
	freeIfNotNull(this->data);
}


ColumnsCatalogRecord* ColumnsCatalogRecord::parse(const int &tableId,
		const Attribute &attrs, const int &position, const int &version,
		const int &isIndexCreated) {
	vector<Attribute> columnsAttrs;
	getColumnsCatalogRecordDescriptor(columnsAttrs);
	vector<string> records;
	records.push_back(to_string(tableId));
	records.push_back(attrs.name);
	records.push_back(to_string(attrs.type));
	records.push_back(to_string(attrs.length));
	records.push_back(to_string(position));
	records.push_back(to_string(version));
	records.push_back(to_string(isIndexCreated));
	void* columnsCatalogRecordData = createTableData(columnsAttrs, records);
	ColumnsCatalogRecord* columnsCatalogRecord = new ColumnsCatalogRecord(columnsCatalogRecordData);
	return columnsCatalogRecord;
}

RC ColumnsCatalogRecord::unParse(int &tableId, Attribute &attrs,
		int &columnIndex, int &version, int &isIndexCreated) {
	vector<Attribute> columnsAttrs;
	getColumnsCatalogRecordDescriptor(columnsAttrs);
	vector<string> records;
	vector<string> columnRecord = getTableData(columnsAttrs, this->data);
	tableId = stoi(columnRecord[0]);
	attrs.name = columnRecord[1];
	attrs.type  = (AttrType)stoi(columnRecord[2]);
	attrs.length = stoi(columnRecord[3]);
	columnIndex = stoi(columnRecord[4]);
	version = stoi(columnRecord[5]);
	isIndexCreated = stoi(columnRecord[6]);
	attrs.isIndex = isIndexCreated;
	return 0;
}

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::invalidateCache(const string &tableName) {
	this->tableNameToRecordDescriptorMap.erase(tableName);
	this->tableNameToRecordDescriptorsMap.erase(tableName);
	return 0;
}

bool RelationManager::isSystemTable(const string &tableName) {
	return tableName == TABLE_CATALOG_NAME || tableName == COLUMNS_CATALOG_NAME;
}

RC RelationManager::createCatalog()
{
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	rbfm->createFile(TABLE_CATALOG_NAME);
	rbfm->createFile(COLUMNS_CATALOG_NAME);

	vector<Attribute> tablesRecordDescriptor;
	vector<Attribute> columnsRecordDescriptor;
	getTablesCatalogRecordDescriptor(tablesRecordDescriptor);
	getColumnsCatalogRecordDescriptor(columnsRecordDescriptor);

	FileHandle fileHandle;
	RID rid;

	// Inserting data in tablesCatalog start

	rbfm->openFile(TABLE_CATALOG_NAME, fileHandle);

	TableCatalogRecord* tableCatalogRecord;
	tableCatalogRecord = TableCatalogRecord::parse(1, TABLE_CATALOG_NAME, 0);
	rbfm->insertRecord(fileHandle, tablesRecordDescriptor, tableCatalogRecord->data, rid);
	delete tableCatalogRecord; // valgrind shows mismatched error // changing it to delete for now

	tableCatalogRecord = TableCatalogRecord::parse(2, COLUMNS_CATALOG_NAME, 0);
	rbfm->insertRecord(fileHandle, tablesRecordDescriptor, tableCatalogRecord->data, rid);
	delete tableCatalogRecord;

	rbfm->closeFile(fileHandle);

	// Inserting data in tablesCatalog ends


	// Inserting data in columnsCatalog starts

	rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);

	ColumnsCatalogRecord* columnsCatalogRecord;
	for (int i = 0; i < tablesRecordDescriptor.size(); ++i) {
		columnsCatalogRecord = ColumnsCatalogRecord::parse(1, tablesRecordDescriptor[i], i, 0, 0);
		rbfm->insertRecord(fileHandle, columnsRecordDescriptor, columnsCatalogRecord->data, rid);
//		rbfm->printRecord(columnsRecordDescriptor, columnsCatalogRecord->data);
		delete columnsCatalogRecord;
	}

	for (int i = 0; i < columnsRecordDescriptor.size(); ++i) {
		columnsCatalogRecord = ColumnsCatalogRecord::parse(2, columnsRecordDescriptor[i], i, 0, 0);
		rbfm->insertRecord(fileHandle, columnsRecordDescriptor, columnsCatalogRecord->data, rid);
//		rbfm->printRecord(columnsRecordDescriptor, columnsCatalogRecord->data);
		delete columnsCatalogRecord;
	}

	rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::deleteCatalog()
{
	this->tableNameToRecordDescriptorMap.clear();
	this->tableNameToRecordDescriptorsMap.clear();
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	return rbfm->destroyFile(TABLE_CATALOG_NAME) & rbfm->destroyFile(COLUMNS_CATALOG_NAME);
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	rbfm->createFile(tableName);
	FileHandle fileHandle;
	RID rid;
	vector<Attribute> tableAttrs;
	vector<Attribute> columnsAttrs;
	getTablesCatalogRecordDescriptor(tableAttrs);
	getColumnsCatalogRecordDescriptor(columnsAttrs);
//	insert row in tables catalog
	rbfm->openFile(TABLE_CATALOG_NAME, fileHandle);
	int tableId = getNextTableId();
	TableCatalogRecord* tableCatalogRecord = TableCatalogRecord::parse(tableId, tableName, 0);
	rbfm->insertRecord(fileHandle, tableAttrs, tableCatalogRecord->data, rid);
	delete tableCatalogRecord;
	rbfm->closeFile(fileHandle);

//	inserting row in columns catalog
	ColumnsCatalogRecord* columnsCatalogRecord;
	rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);
	for (int i = 0; i < attrs.size(); ++i) {
		columnsCatalogRecord = ColumnsCatalogRecord::parse(tableId, attrs[i], i, 0, 0);
		rbfm->insertRecord(fileHandle, columnsAttrs, columnsCatalogRecord->data, rid);
		delete columnsCatalogRecord;

	}
	rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	bool isSystemTable = RelationManager::isSystemTable(tableName);
	if(isSystemTable) {
		return -1;
	}
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbfm->openFile(TABLE_CATALOG_NAME, fileHandle);
	RBFM_ScanIterator * iterator = new RBFM_ScanIterator();

	vector<Attribute> tableCatalogAttrs;
	getTablesCatalogRecordDescriptor(tableCatalogAttrs);

	Attribute tableNameAttr = tableCatalogAttrs[1];
	Attribute tableIdAttr = tableCatalogAttrs[0];

	vector<string> projections;
	vector<Attribute> projectedAttribute;

	rbfm->destroyFile(tableName.c_str());


	RID rid;
	void* tablesCatalogRecord = malloc(1000);
	projections.push_back(tableIdAttr.name);
	projectedAttribute.push_back(tableIdAttr);

	VarcharParser* vp = VarcharParser::parse(tableName);
	rbfm->scan(fileHandle, tableCatalogAttrs, tableNameAttr.name, EQ_OP, vp->data, projections, *iterator);
	int rc = iterator->getNextRecord(rid, tablesCatalogRecord);
	delete iterator;
	if(rc == -1) {
		freeIfNotNull(tablesCatalogRecord);
		return rc;
	}

	vector<string> tablesCatalogParsedRecord = getTableData(projectedAttribute,
			tablesCatalogRecord);
	freeIfNotNull(tablesCatalogRecord);
	string tableId = tablesCatalogParsedRecord[0];

	vector<RID> RIDVector;
	RID ridColumn;
	rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);
	RBFM_ScanIterator *iterator1 = new RBFM_ScanIterator();
	vector<Attribute> columnsCatalogAttrs;
	getColumnsCatalogRecordDescriptor(columnsCatalogAttrs);

	vector<string> colmnsCatalogAttrsNames;
	for (int i = 0; i < columnsCatalogAttrs.size(); ++i) {
		Attribute attr = columnsCatalogAttrs[i];
		colmnsCatalogAttrsNames.push_back(attr.name);
	}

	int tableIdInt = stoi(tableId);
	rbfm->scan(fileHandle, columnsCatalogAttrs, tableIdAttr.name, EQ_OP,
			&tableIdInt, colmnsCatalogAttrsNames, *iterator1);
	void* columnsCatalogRecord = malloc(2000);

	while (iterator1->getNextRecord(ridColumn, columnsCatalogRecord) != -1) {
		RIDVector.push_back(ridColumn);
	}
	freeIfNotNull(columnsCatalogRecord);
	delete iterator1;
	for (int i = 0; i < RIDVector.size(); i++) {
		ridColumn = RIDVector.at(i);
		this->internalDeleteTuple(COLUMNS_CATALOG_NAME, ridColumn);

	}

	// Drop table
	this->internalDeleteTuple(TABLE_CATALOG_NAME, rid);
	this->invalidateCache(tableName);
	return 0;
}

RC RelationManager:: getTableDetailsByName(const string &tableName, int &tableId, int &versionId) {
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
		FileHandle fileHandle;
		rbfm->openFile(TABLE_CATALOG_NAME, fileHandle);
		RBFM_ScanIterator * iterator = new RBFM_ScanIterator();
		vector<Attribute> tableCatalogAttrs;
		getTablesCatalogRecordDescriptor(tableCatalogAttrs);
		Attribute tableNameAttr = tableCatalogAttrs[1];
		Attribute tableIdAttr = tableCatalogAttrs[0];
		Attribute tableversionAttr = tableCatalogAttrs[3];
		vector<string> projections;
		vector<Attribute> projectedAttribute;
		RID rid;
		void* tablesCatalogRecord= malloc(PAGE_SIZE);
		projections.push_back(tableIdAttr.name);
		projections.push_back(tableversionAttr.name);
		projectedAttribute.push_back(tableIdAttr);
		projectedAttribute.push_back(tableversionAttr);
		VarcharParser * vp = VarcharParser::parse(tableName);
		rbfm->scan(fileHandle, tableCatalogAttrs, tableNameAttr.name, EQ_OP, vp->data, projections, *iterator);
		int rc = iterator->getNextRecord(rid, tablesCatalogRecord);
		delete iterator;
		if(rc == -1) {
			freeIfNotNull(tablesCatalogRecord);
			return rc;
		}

		vector<string> tablesCatalogParsedRecord  = getTableData(projectedAttribute, tablesCatalogRecord);
		tableId = stoi(tablesCatalogParsedRecord[0]);
		versionId = stoi(tablesCatalogParsedRecord[1]);
		freeIfNotNull(tablesCatalogRecord);
		return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	if(this->tableNameToRecordDescriptorMap.find(tableName) != this->tableNameToRecordDescriptorMap.end()) {
		attrs = this->tableNameToRecordDescriptorMap[tableName];
		return 0;
	}
	int rc;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	RID rid;
	FileHandle fileHandle;
	int tableIdInt, versionId;
	rc = this->getTableDetailsByName(tableName, tableIdInt, versionId);
	if(rc == -1) {
		return rc;
	}
	rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);
	RBFM_ScanIterator * iterator;
	iterator = new RBFM_ScanIterator();
	vector<Attribute> columnsCatalogAttrs;
	getColumnsCatalogRecordDescriptor(columnsCatalogAttrs);

	vector<string> colmnsCatalogAttrsNames;
	for (int i = 0; i < columnsCatalogAttrs.size(); ++i) {
		Attribute attr = columnsCatalogAttrs[i];
		colmnsCatalogAttrsNames.push_back(attr.name);
	}

	rbfm->scan(fileHandle, columnsCatalogAttrs, "table-id", EQ_OP, &tableIdInt  , colmnsCatalogAttrsNames, *iterator);
	void* columnsCatalogRecord = malloc(2000);
	map<int, Attribute> columnIndexToAttributeMap;
	while(iterator->getNextRecord(rid, columnsCatalogRecord) != -1) {
		ColumnsCatalogRecord* ccr = new ColumnsCatalogRecord(columnsCatalogRecord);
//		rbfm->printRecord(columnsCatalogAttrs, columnsCatalogRecord);
		int readTableId;
		Attribute readAttribute;
		int columnIndex;
		int readVersion;
		int isIndexCreated;
		ccr->unParse(readTableId, readAttribute, columnIndex, readVersion, isIndexCreated);
		if(versionId == readVersion) {
			columnIndexToAttributeMap[columnIndex] = readAttribute;
		}
		ccr->data = 0;
		delete ccr;
	}

	for (auto iter = columnIndexToAttributeMap.begin(); iter != columnIndexToAttributeMap.end(); iter++)
	{
	    attrs.push_back(iter->second);
	}

	freeIfNotNull(columnsCatalogRecord);
	this->tableNameToRecordDescriptorMap[tableName] = attrs;
	//free(columnsCatalogRecord);
	delete iterator;
    return 0;
}

RC RelationManager::getAttributesVector(const string &tableName, vector<vector<Attribute>> &recordDescriptors) {
	if(this->tableNameToRecordDescriptorsMap.find(tableName) != this->tableNameToRecordDescriptorsMap.end()) {
			recordDescriptors = this->tableNameToRecordDescriptorsMap[tableName];
			return 0;
    }
	int rc;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	map<int, map<int, Attribute>> versionToRecordDescriptorMap;
		RID rid;
		FileHandle fileHandle;
		int tableIdInt, versionId;
		rc = this->getTableDetailsByName(tableName, tableIdInt, versionId);
		if(rc == -1) {
			return rc;
		}
		rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);
		RBFM_ScanIterator * iterator;
		iterator = new RBFM_ScanIterator();
		vector<Attribute> columnsCatalogAttrs;
		getColumnsCatalogRecordDescriptor(columnsCatalogAttrs);

		vector<string> colmnsCatalogAttrsNames;
		for (int i = 0; i < columnsCatalogAttrs.size(); ++i) {
			Attribute attr = columnsCatalogAttrs[i];
			colmnsCatalogAttrsNames.push_back(attr.name);
		}

		rbfm->scan(fileHandle, columnsCatalogAttrs, "table-id", EQ_OP, &tableIdInt  , colmnsCatalogAttrsNames, *iterator);
		void* columnsCatalogRecord = malloc(2000);
		while(iterator->getNextRecord(rid, columnsCatalogRecord) != -1) {
			ColumnsCatalogRecord* ccr = new ColumnsCatalogRecord(columnsCatalogRecord);
			int readTableId;
			Attribute readAttribute;
			int columnIndex;
			int readVersionId;
			int isIndexCreated;
			ccr->unParse(readTableId, readAttribute, columnIndex, readVersionId, isIndexCreated);
			ccr->data = 0;
			delete ccr;
			if(versionToRecordDescriptorMap.find(readVersionId) == versionToRecordDescriptorMap.end()) {
				map<int, Attribute> recordDescriptor;
				recordDescriptor[columnIndex] = readAttribute;
				versionToRecordDescriptorMap[readVersionId] = recordDescriptor;
			} else {
				versionToRecordDescriptorMap[readVersionId][columnIndex] = readAttribute;
			}
		}
		freeIfNotNull(columnsCatalogRecord);

		for(auto it = versionToRecordDescriptorMap.begin(); it != versionToRecordDescriptorMap.end(); ++it)
		{
			vector<Attribute> recordDescriptor;
			map<int, Attribute>columnOffsetToAttributeMap = it->second;
			for(auto it2=columnOffsetToAttributeMap.begin(); it2 != columnOffsetToAttributeMap.end(); ++it2) {
				recordDescriptor.push_back(it2->second);
			}
			recordDescriptors.push_back(recordDescriptor);
		}
		delete iterator;
		this->tableNameToRecordDescriptorsMap[tableName] = recordDescriptors;
	    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    bool isSystemTable = RelationManager::isSystemTable(tableName);
    if(isSystemTable) {
        return -1;
    }
    return this->internalInsertTuple(tableName, data, rid);
}


RC RelationManager::internalInsertTuple(const string &tableName, const void *data, RID &rid)
{
	int rc;
	vector<vector<Attribute>> recordDescriptors;
	FileHandle fileHandle;
	int currentVersion;
	int tableId;
	rc = this->getTableDetailsByName(tableName, tableId, currentVersion);
	if(rc == -1) {
		return rc;
	}
	this->getAttributesVector(tableName, recordDescriptors);
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->openFile(tableName, fileHandle);
	rc = rbfm->internalInsertRecord(fileHandle, recordDescriptors, data, rid, currentVersion, 0);
	if(rc == -1) {
		return rc;
	}
	vector<Attribute> indexAttrs;
	Attribute indexAttr;
	InternalRecord* record = InternalRecord::parse(recordDescriptors[currentVersion], data, currentVersion, false);
	this->getAttributes(tableName, indexAttrs);
	for(int i=0; i<indexAttrs.size(); i++) {
		indexAttr = indexAttrs[i];
		if(indexAttr.isIndex == 0 )
			continue;
		void* key = malloc(indexAttr.length+sizeof(int));
		IndexManager* im = IndexManager::instance();
		bool isNull;
		record->getAttributeByIndex(i, recordDescriptors[currentVersion], key, isNull);
		IXFileHandle ixhandle;
		rc = im->openFile(IndexManager::getIndexFileName(tableName, indexAttr.name), ixhandle);
		im->insertEntry(ixhandle, indexAttr, key, rid);
//        im->printBtree(ixhandle, indexAttr);
        im->closeFile(ixhandle);
        freeIfNotNull(key);
	}
	delete record;
	return rbfm->closeFile(fileHandle);
}

RC RelationManager::internalDeleteTuple(const string &tableName, const RID &rid) {
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    rbfm->openFile(tableName, fileHandle);
    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    int rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    if(rc == -1) {
        return rc;
    }
    rc = rbfm->closeFile(fileHandle);
    return rc;
}


RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    bool isSystemTable = RelationManager::isSystemTable(tableName);
    if (isSystemTable) {
        return -1;
    }
    return this->internalDeleteTuple(tableName, rid);
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid){
	bool isSystemTable = RelationManager::isSystemTable(tableName);
	if(isSystemTable) {
		return -1;
	}
	return this->internalUpdateTuple(tableName, data, rid);
}

RC RelationManager::internalUpdateTuple(const string &tableName, const void *data, const RID &rid)
{
	int rc;
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    rbfm->openFile(tableName, fileHandle);
    int tableId, versionId;
    rc = this->getTableDetailsByName(tableName, tableId, versionId);
    if(rc == -1) {
    	return rc;
    }
    vector<vector<Attribute>> recordDescriptors;
    this->getAttributesVector(tableName, recordDescriptors);
    rc = rbfm->internalUpdateRecord(fileHandle, recordDescriptors, data, rid, versionId);
    if(rc == -1) {
        return rc;
    }
    return rbfm->closeFile(fileHandle);
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	int rc;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbfm->openFile(tableName, fileHandle);
	int tableId, versionId;
	rc = this->getTableDetailsByName(tableName, tableId, versionId);
	if(rc == -1) {
		return rc;
	}
	vector<vector<Attribute>> recordDescriptors;
	this->getAttributesVector(tableName, recordDescriptors);
	vector<Attribute> attrs = recordDescriptors[versionId];
	vector<string> attributeNames;
	Attribute attr;
	for(int i=0; i<attrs.size(); i++) {
		attr = attrs[i];
		attributeNames.push_back(attr.name);
	}
    rc = rbfm->internalReadAttributes(fileHandle, recordDescriptors, rid,
    		attributeNames, data, versionId);
    if(rc == -1) {
        return rc;
    }
    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	int rc;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbfm->openFile(tableName, fileHandle);
	int tableId, versionId;
	rc = this->getTableDetailsByName(tableName, tableId, versionId);
	if(rc == -1) {
		return rc;
	}
	vector<vector<Attribute>> recordDescriptors;
	vector<Attribute> attrs;
	vector<string> attributeNames;
	this->getAttributesVector(tableName, recordDescriptors);
	rc = rbfm->internalReadAttribute(fileHandle, recordDescriptors, rid, attributeName, data, versionId);
    if(rc == -1) {
        return rc;
    }
    return rbfm->closeFile(fileHandle);
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	int rc;
   RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
   FileHandle* fileHandle = new FileHandle();
   vector<vector<Attribute>> recordDescriptors;
   RBFM_ScanIterator* rbfm_ScanIterator = new RBFM_ScanIterator();
   RBFM_ScanIterator rbfm_ScanIteratorInstance = *rbfm_ScanIterator;
   rc = rbfm->openFile(tableName, *fileHandle);
   if(rc == -1) {
	   return rc;
   }
   int tableId, versionId;
   	rc = this->getTableDetailsByName(tableName, tableId, versionId);
   	if(rc == -1) {
   		return rc;
   	}
   	this->getAttributesVector(tableName, recordDescriptors);
   	rm_ScanIterator.rbfmIterator = rbfm_ScanIterator;
   	rbfm->internalScan(*fileHandle, recordDescriptors, conditionAttribute, compOp, value, attributeNames, *rbfm_ScanIterator, versionId);
   //	delete rbfm_ScanIterator;
   	return 0;
}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return this->rbfmIterator->getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	return this->rbfmIterator->close();
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	bool isSystemTable = RelationManager::isSystemTable(tableName);
	if(isSystemTable) {
		return -1;
	}
	vector<Attribute> currentRecordDescriptor;
	vector<Attribute> newRecordDescriptor;
	int rc, tableId, versionId;
	Attribute tempAttr;
	ColumnsCatalogRecord* columnsCatalogRecord;
	vector<string> projections;
	RID rid;
	projections.push_back("table-name");
	void* record = malloc(PAGE_SIZE);
	rc = this->getAttributes(tableName, currentRecordDescriptor);
	if(rc == -1) {
		return rc;
	}
	this->getTableDetailsByName(tableName, tableId, versionId);

	for (int i = 0; i < currentRecordDescriptor.size(); ++i) {
		tempAttr = currentRecordDescriptor[i];
		if(tempAttr.name != attributeName) {
			newRecordDescriptor.push_back(tempAttr);
		}
	}

	for(int i=0; i< newRecordDescriptor.size(); i++) {
		tempAttr = newRecordDescriptor[i];
		columnsCatalogRecord = ColumnsCatalogRecord::parse(tableId, tempAttr, i, versionId+1, 0);
		this->internalInsertTuple(COLUMNS_CATALOG_NAME, columnsCatalogRecord->data, rid);
		delete columnsCatalogRecord;
	}

	RM_ScanIterator *iter = new RM_ScanIterator();
	VarcharParser *vp = VarcharParser::parse(tableName);
	this->scan(TABLE_CATALOG_NAME, "table-name", EQ_OP, vp->data , projections, *iter);
	RID tablesRowRid;
	rc = iter->getNextTuple(tablesRowRid, record);

	TableCatalogRecord* newRecord = TableCatalogRecord::parse(tableId, tableName, versionId+1);
	if(rc == -1) {
		return rc;
	}
	delete (iter);
	freeIfNotNull(record);
	rc = this->internalUpdateTuple(TABLE_CATALOG_NAME, newRecord->data, tablesRowRid);
	delete newRecord;
	this->invalidateCache(tableName);
	return rc;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	bool isSystemTable = RelationManager::isSystemTable(tableName);
	if(isSystemTable) {
		return -1;
	}
	vector<Attribute> currentRecordDescriptor;
	int rc, tableId, versionId;
	Attribute tempAttr;
	ColumnsCatalogRecord* columnsCatalogRecord;
	vector<string> projections;
	RID rid;
	projections.push_back("table-name");
	void* record = malloc(PAGE_SIZE);
	rc = this->getAttributes(tableName, currentRecordDescriptor);
	if(rc == -1) {
		return rc;
	}
	this->getTableDetailsByName(tableName, tableId, versionId);
	currentRecordDescriptor.push_back(attr);

	for(int i=0; i< currentRecordDescriptor.size(); i++) {
		tempAttr = currentRecordDescriptor[i];
		columnsCatalogRecord = ColumnsCatalogRecord::parse(tableId, tempAttr, i, versionId+1, 0);
		this->internalInsertTuple(COLUMNS_CATALOG_NAME, columnsCatalogRecord->data, rid);
		delete columnsCatalogRecord;
	}

	RM_ScanIterator *iter = new RM_ScanIterator();

	VarcharParser *varcharParser = VarcharParser::parse(tableName);
	this->scan(TABLE_CATALOG_NAME, "table-name", EQ_OP, varcharParser->data, projections, *iter);
	RID tablesRowRid;
	rc = iter->getNextTuple(tablesRowRid, record);

	TableCatalogRecord* newRecord = TableCatalogRecord::parse(tableId, tableName, versionId+1);
	if(rc == -1) {
		return rc;
	}
	rc = this->internalUpdateTuple(TABLE_CATALOG_NAME, newRecord->data, tablesRowRid);
	delete iter;
	freeIfNotNull(record);
	delete newRecord;
	this->invalidateCache(tableName);
	return rc;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName){
	int rc = 0;
	int tableId, versionId;
	this->getTableDetailsByName(tableName, tableId, versionId);
	vector<Attribute> recordDescriptors;
    getColumnsCatalogRecordDescriptor(recordDescriptors);
	RM_ScanIterator iter;
	vector<string> attrNames;
	for(int i=0; i<recordDescriptors.size(); i++) {
		attrNames.push_back(recordDescriptors[i].name);
	}
	string tableIdLabel = "table-id";
	this->scan(COLUMNS_CATALOG_NAME, tableIdLabel, EQ_OP, &tableId, attrNames, iter);
	RID rid;
	void* data = malloc(PAGE_SIZE);
	ColumnsCatalogRecord * record;
	while(iter.getNextTuple(rid, data) != -1){
		record = new ColumnsCatalogRecord(data);
		Attribute attr;
		int tableId;
		int columnIndex;
		int version;
		int isIndexCreated = 0;
		record->unParse(tableId, attr, columnIndex, version, isIndexCreated);
		record->data = NULL;
		delete record;
		if(attr.name != attributeName) {
			continue;
		}
		if(isIndexCreated == 1) {
//			Index already created
			freeIfNotNull(data);
			return -1;
		}
		else {
//			update entry in columns table for this
			ColumnsCatalogRecord* updatedRecord = ColumnsCatalogRecord::parse(tableId, attr, columnIndex, version, 1);
			rc = this->internalUpdateTuple(COLUMNS_CATALOG_NAME, updatedRecord->data, rid);
			delete updatedRecord;
			if(rc == -1) {
				freeIfNotNull(data);
				return rc;
			}
			string indexName = IndexManager::getIndexFileName(tableName, attributeName);
			IndexManager* im = IndexManager::instance();
			rc = im->createFile(indexName);
			if(rc == -1) {
				return -1;
			}

			IXFileHandle ixFileHandle;
			im->openFile(indexName, ixFileHandle);
			vector<string> indexKey = {attributeName};
			vector<Attribute> indexAttr = {attr};
			iter.close();

			RM_ScanIterator iter2;
			this->scan(tableName, attributeName, NO_OP, NULL, indexKey, iter2);

			while(iter2.getNextTuple(rid, data) != -1) {
				im->insertEntry(ixFileHandle, attr, (char*)data+1, rid);
			}
            iter2.close();
            im->closeFile(ixFileHandle);
			freeIfNotNull(data);
			this->invalidateCache(tableName);
			return rc;

		}
	}
	freeIfNotNull(data);
	return -1;
}

RC RelationManager::getAllIndexOnTable(const string tableName, vector<Attribute> &attrs) {
	vector<Attribute> allAttrs;
	Attribute attr;
	this->getAttributes(tableName, allAttrs);
	for(int i=0; i < allAttrs.size(); i++) {
		attr = allAttrs[i];
		if(attr.isIndex) {
			attrs.push_back(attr);
		}
	}
	return 0;
}

RC RelationManager::indexScan(const string &tableName,
			const string &attributeName,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive,
			RM_IndexScanIterator &rm_IndexScanIterator) {

	IndexManager* ix = IndexManager::instance();
	IXFileHandle * ixFileHandle = new IXFileHandle();
	IX_ScanIterator *iterator = new IX_ScanIterator();
	rm_IndexScanIterator.indexHandle = ixFileHandle;
	rm_IndexScanIterator.ixIterator = iterator;
	vector<Attribute> attrs;
	this->getAllIndexOnTable(tableName, attrs);
	ix->openFile(IndexManager::getIndexFileName(tableName, attributeName), *ixFileHandle);
	for(int i=0; i<attrs.size(); i++) {
        rm_IndexScanIterator.attr = attrs[i];
		if(rm_IndexScanIterator.attr.name == attributeName) {
			break;
		}
		if(i == attrs.size()-1) {
//			Index not found
			return -1;
		}
	}
//    ix->printBtree(*ixFileHandle, indexAttr);

    return ix->scan(*ixFileHandle, rm_IndexScanIterator.attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iterator);

}
