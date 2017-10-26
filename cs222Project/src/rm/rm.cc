
#include "rm.h"
#include <tuple>
#include <math.h>

#define TABLE_CATALOG_NAME "Tables"
#define COLUMNS_CATALOG_NAME "Columns"

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
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> attrs;
	getTablesCatalogRecordDescriptor(attrs);
	vector<string> projectedAttributes;
	string queryName = attrs[0].name;
	projectedAttributes.push_back(queryName);
	rbfm->openFile(TABLE_CATALOG_NAME, fileHandle);
    RBFM_ScanIterator* iterator = new RBFM_ScanIterator();
	rbfm->scan(fileHandle, attrs, queryName, NO_OP, 0, projectedAttributes, *iterator);
	int count = 0;
	RID rid;
	void* returned_data = malloc(1000);
	while(iterator->getNextRecord(rid, returned_data) != -1) {
		count++;
	}
//	#TODO
//	delete iterator;
	rbfm->closeFile(fileHandle);
	return count+1;
}

TableCatalogRecord::TableCatalogRecord(void * data) {
	this->data = data;
}


TableCatalogRecord* TableCatalogRecord::parse(const int &id, const string &tableName) {
	vector<Attribute> attrs;
	getTablesCatalogRecordDescriptor(attrs);
	vector<string> tableRecord;
	tableRecord.push_back(to_string(id));
	tableRecord.push_back(tableName);
	tableRecord.push_back(tableName);
	void* tableCatalogRecordData = createTableData(attrs, tableRecord);
	TableCatalogRecord* record = new TableCatalogRecord(tableCatalogRecordData);
	return record;
}

RC TableCatalogRecord::unParse(int &id, string &tableName) {

}

ColumnsCatalogRecord::ColumnsCatalogRecord(void * data) {
	this->data = data;
}


ColumnsCatalogRecord* ColumnsCatalogRecord::parse(const int &tableId, const Attribute &attrs, const int &position) {
	vector<Attribute> columnsAttrs;
	getColumnsCatalogRecordDescriptor(columnsAttrs);
	vector<string> records;
	records.push_back(to_string(tableId));
	records.push_back(attrs.name);
	records.push_back(to_string(attrs.type));
	records.push_back(to_string(attrs.length));
	records.push_back(to_string(position));
	void* columnsCatalogRecordData = createTableData(columnsAttrs, records);
	ColumnsCatalogRecord* columnsCatalogRecord = new ColumnsCatalogRecord(columnsCatalogRecordData);
	return columnsCatalogRecord;
}

RC ColumnsCatalogRecord::unParse(int &tableId, Attribute &attrs, int &columnIndex) {
	vector<Attribute> columnsAttrs;
	getColumnsCatalogRecordDescriptor(columnsAttrs);
	vector<string> records;
	vector<string> columnRecord = getTableData(columnsAttrs, this->data);
	tableId = stoi(columnRecord[0]);
	attrs.name = columnRecord[1];
	attrs.type  = (AttrType)stoi(columnRecord[2]);
	attrs.length = stoi(columnRecord[3]);
	columnIndex = stoi(columnRecord[4]);
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
	tableCatalogRecord = TableCatalogRecord::parse(1, TABLE_CATALOG_NAME);
	rbfm->insertRecord(fileHandle, tablesRecordDescriptor, tableCatalogRecord->data, rid);
	free(tableCatalogRecord);

	tableCatalogRecord = TableCatalogRecord::parse(2, COLUMNS_CATALOG_NAME);
	rbfm->insertRecord(fileHandle, tablesRecordDescriptor, tableCatalogRecord->data, rid);
	free(tableCatalogRecord);

	rbfm->closeFile(fileHandle);

	// Inserting data in tablesCatalog ends


	// Inserting data in columnsCatalog starts

	vector<vector<string>> records;
	vector<string> columnsRecord;
	rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);

	ColumnsCatalogRecord* columnsCatalogRecord;
	for (int i = 0; i < tablesRecordDescriptor.size(); ++i) {
		columnsCatalogRecord = ColumnsCatalogRecord::parse(1, tablesRecordDescriptor[i], i);
		rbfm->insertRecord(fileHandle, columnsRecordDescriptor, columnsCatalogRecord->data, rid);
		rbfm->printRecord(columnsRecordDescriptor, columnsCatalogRecord->data);
		free(columnsCatalogRecord);
	}

	for (int i = 0; i < columnsRecordDescriptor.size(); ++i) {
		columnsCatalogRecord = ColumnsCatalogRecord::parse(2, columnsRecordDescriptor[i], i);
		rbfm->insertRecord(fileHandle, columnsRecordDescriptor, columnsCatalogRecord->data, rid);
		rbfm->printRecord(columnsRecordDescriptor, columnsCatalogRecord->data);
		free(columnsCatalogRecord);
	}

	rbfm->closeFile(fileHandle);

    return 0;
}

RC RelationManager::deleteCatalog()
{
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	return rbfm->destroyFile(TABLE_CATALOG_NAME) && rbfm->destroyFile(COLUMNS_CATALOG_NAME);
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
	TableCatalogRecord* tableCatalogRecord = TableCatalogRecord::parse(tableId, tableName);
	rbfm->insertRecord(fileHandle, tableAttrs, tableCatalogRecord->data, rid);
	rbfm->closeFile(fileHandle);

//	inserting row in columns catalog
	ColumnsCatalogRecord* columnsCatalogRecord;
	rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);
	for (int i = 0; i < attrs.size(); ++i) {
		columnsCatalogRecord = ColumnsCatalogRecord::parse(tableId, attrs[i], i);
		rbfm->insertRecord(fileHandle, columnsAttrs, columnsCatalogRecord->data, rid);
		free(columnsCatalogRecord);
	}

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

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
	RID rid;
	void* tablesCatalogRecord= malloc(1000);
	projections.push_back(tableIdAttr.name);
	projectedAttribute.push_back(tableIdAttr);

	rbfm->scan(fileHandle, tableCatalogAttrs, tableNameAttr.name, EQ_OP, (void*)tableName.c_str(), projections, *iterator);
	int rc = iterator->getNextRecord(rid, tablesCatalogRecord);
	delete iterator;
	if(rc == -1) {
		return rc;
	}

	vector<string> tablesCatalogParsedRecord  = getTableData(projectedAttribute, tablesCatalogRecord);
	string tableId = tablesCatalogParsedRecord[0];

	rbfm->openFile(COLUMNS_CATALOG_NAME, fileHandle);
	iterator = new RBFM_ScanIterator();
	vector<Attribute> columnsCatalogAttrs;
	getColumnsCatalogRecordDescriptor(columnsCatalogAttrs);

	vector<string> colmnsCatalogAttrsNames;
	for (int i = 0; i < columnsCatalogAttrs.size(); ++i) {
		Attribute attr = columnsCatalogAttrs[i];
		colmnsCatalogAttrsNames.push_back(attr.name);
	}

	int tableIdInt = stoi(tableId);
	rbfm->scan(fileHandle, columnsCatalogAttrs, tableIdAttr.name, EQ_OP, &tableIdInt  , colmnsCatalogAttrsNames, *iterator);
	void* columnsCatalogRecord = malloc(2000);
	while(iterator->getNextRecord(rid, columnsCatalogRecord) != -1) {
		ColumnsCatalogRecord* ccr = new ColumnsCatalogRecord(columnsCatalogRecord);
//		rbfm->printRecord(columnsCatalogAttrs, columnsCatalogRecord);
		int readTableId;
		Attribute readAttribute;
		int columnIndex;
		ccr->unParse(readTableId, readAttribute, columnIndex);
		attrs.push_back(readAttribute);
	}
	free(columnsCatalogRecord);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	vector<Attribute> attributes;
	FileHandle fileHandle;
	this->getAttributes(tableName, attributes);
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	rbfm->openFile(tableName, fileHandle);
	return rbfm->insertRecord(fileHandle, attributes, data, rid);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    vector<Attribute> attrs;
    FileHandle fileHandle;
    this->getAttributes(tableName, attrs);
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    rbfm->openFile(tableName, fileHandle);
    return rbfm->readRecord(fileHandle, attrs, rid, data);
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    vector<Attribute> attrs;
    rbfm->openFile(tableName, fileHandle);
    this->getAttributes(tableName, attrs);
    return rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data);
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
   RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
   FileHandle fileHandle;
   vector<Attribute> attrs;
   RBFM_ScanIterator* iterator = new RBFM_ScanIterator();
   rbfm->openFile(tableName, fileHandle);
   this->getAttributes(tableName, attrs);
   rm_ScanIterator.rbfmIterator = *iterator;
   return rbfm->scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames, *iterator);
}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return this->rbfmIterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	return this->rbfmIterator.close();
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


