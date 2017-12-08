
#include "qe.h"
#include "time.h"

Filter::Filter(Iterator *input, const Condition & condition ){
	this->iterator = input;
	this->condition = condition;
}

Filter::~Filter(){
	this->iterator = 0;
}


RC Filter::getNextTuple(void* data) {
	while(iterator->getNextTuple(data) != EOF) {
		if(compareCondition(data) == 0) {
			return 0;
		}
	}
	return -1;

}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	this->iterator->getAttributes(attrs);
}

RC Filter::compareCondition(void *data){
	int comparisonResult = -1;
	vector<Attribute> attr;
	this->getAttributes(attr);
	bool isNULL;
	void *leftAttribute = malloc(PAGE_SIZE);
	for (int index = 0; index < attr.size(); index++) {

		if (attr.at(index).name == this->condition.lhsAttr) {

			InternalRecord *ir = InternalRecord::parse(attr, data, 0, false);
			memset(leftAttribute, 0, PAGE_SIZE);
			ir->getAttributeByIndex(index, attr, leftAttribute, isNULL);

            if (this->condition.bRhsIsAttr) {
				for (int index = 0; index < attr.size(); index++) {

					if (attr.at(index).name == this->condition.rhsAttr) {
						InternalRecord *ir = InternalRecord::parse(attr, data,
								0, false);
						void *rightAttribute = malloc(PAGE_SIZE);
						memset(rightAttribute, 0, PAGE_SIZE);
						ir->getAttributeByIndex(index, attr, rightAttribute,isNULL);
						if(!isNULL){
							comparisonResult = compareAttributes(rightAttribute,leftAttribute, attr.at(index),this->condition.op);
							freeIfNotNull(leftAttribute);
							return comparisonResult;
						}
					}
				}

			}
			else{
			comparisonResult = compareAttributes(condition.rhsValue.data,leftAttribute, attr.at(index), this->condition.op);
			freeIfNotNull(leftAttribute);
			return comparisonResult;
			}

		}
	}
	freeIfNotNull(leftAttribute);
	return comparisonResult;
}

Project::Project(Iterator *input,const vector<string> &attrNames){
	this->iterator = input;
	vector <Attribute> attr;
	this->iterator->getAttributes(attr);
	for(string attrName :  attrNames){
		for(int i=0; i< attr.size(); i++){
			if(attr.at(i).name == attrName){
				projectedAttr.push_back(attr.at(i));
				break;
			}
		}
	}
}

RC Project::getNextTuple(void* data){
	int rc;
	void *nextTuple = calloc(1,PAGE_SIZE);

	rc = this->iterator->getNextTuple(nextTuple);
	if(rc == -1) {
		free(nextTuple);
		return -1;
	}

	bool isNull;
	vector <Attribute> attrs;
	this->iterator->getAttributes(attrs);
	vector <void*> dataArray;
	vector <bool> isNullArray;
	vector<Attribute> newRecordDescriptor;
	this->getAttributes(newRecordDescriptor);

	InternalRecord* ir = InternalRecord::parse(attrs, nextTuple, 0, false);

	for(int i=0; i<newRecordDescriptor.size(); i++){
		void *projectedData = calloc(1,PAGE_SIZE);
		bool isNull;
		Attribute attr = newRecordDescriptor[i];
		ir->getAttributeValueByName(attr.name,attrs,projectedData,isNull);
		dataArray.push_back(projectedData);
		isNullArray.push_back(isNull);
	}
	mergeAttributesData(projectedAttr, isNullArray, dataArray,data);
	return 0;
}


void Project::getAttributes(vector<Attribute> &attrs)const{

	attrs.clear();
	attrs= this->projectedAttr;
}


Aggregate::Aggregate(Iterator *input,  Attribute aggAttr, AggregateOp op){
	this->iterator = input;
	this->aggAttr = aggAttr;
	this->oper = op;
	this->reachedEndOfFile = false;
	vector <Attribute> attr;
	this->iterator->getAttributes(attr);
		for(int i = 0; i< attr.size();i++){
			if(attr.at(i).name == aggAttr.name){
				this->index = i;
			}
		}
};

RC Aggregate::getNextTuple(void *data){

	void *nextTuple = calloc(1,PAGE_SIZE);
	vector <Attribute> attribute;
	this->iterator->getAttributes(attribute);
	bool areAllAtrributesNull = true;
	bool nullbit = false;
	float counter = 0;
	float sum = 0;
	float finalResult ;

	if(this->reachedEndOfFile){
		 return -1;
	}
	 switch (this->oper) {
	 	 case MIN:
	 	 case MAX :
	 	 {
	 		bool isNull;
	 		void *maxData = calloc(1,PAGE_SIZE);
	 		bool firstCount = false;

	 			while(this->iterator->getNextTuple(nextTuple)!=EOF){
	 				for(int index= 0; index<attribute.size();index++){
	 					if(attribute.at(index).name == this->aggAttr.name ){
	 							InternalRecord *rf = InternalRecord::parse(attribute,nextTuple,0,false);
	 							void* attributeData =  calloc (1,PAGE_SIZE);
	 							rf->getAttributeByIndex(index,attribute,attributeData,isNull);
	 								if(!isNull){
	 									if(!firstCount){
	 										memcpy(maxData,attributeData,sizeof(int));
	 										firstCount = true;
	 									}
	 										float comparisonResult = compare(attributeData,maxData, attribute.at(index),false, false);
	 										if(comparisonResult < 0 && this->oper == MAX){
	 											memcpy(maxData,attributeData,sizeof(int));
	 											freeIfNotNull(attributeData);
	 											areAllAtrributesNull = false;

	 										}
	 										else if (comparisonResult > 0 && this->oper == MIN){
	 											memcpy(maxData,attributeData,sizeof(int));
	 											freeIfNotNull(attributeData);
	 											areAllAtrributesNull = false;
	 										}
	 								}
	 							}
	 					}
	 			}
	 			char * cursorAggregate = (char*)data;
	 			if(areAllAtrributesNull ){
	 				nullbit = true;
	 			}
	 			memcpy(cursorAggregate, &nullbit, sizeof(bool));
	 			cursorAggregate = cursorAggregate + 1;
	 			if(this->aggAttr.type == TypeInt){
	 				finalResult = (float)*(int*)maxData;
	 			}
	 			memcpy(cursorAggregate, &finalResult,sizeof(int));
	 			this->reachedEndOfFile = true;
	 			free(maxData);
	 	 	 }
	 			break;

	 	 case COUNT :
	 	 {

	 		int RC = this->sumAndCountAggregrate(&sum,&counter,areAllAtrributesNull);
	 		char * cursorAggregate = (char*) data;
	 		if (areAllAtrributesNull) {
	 			 nullbit = true;
	 		}
	 		memcpy(cursorAggregate, &nullbit, sizeof(bool));
	 		cursorAggregate = cursorAggregate + 1;
	 		finalResult =  counter;
	 		memcpy(cursorAggregate, &finalResult, sizeof(int));
 			this->reachedEndOfFile = true;

	 	 }
	 		break;

	 	 case AVG :
	 	 {
	 		this->sumAndCountAggregrate(&sum, &counter, areAllAtrributesNull);
	 		char * cursorAggregate = (char*) data;
	 		if (areAllAtrributesNull) {
	 			 	nullbit = true;
	 		}
	 		memcpy(cursorAggregate, &nullbit, sizeof(bool));
	 		cursorAggregate = cursorAggregate + 1;
	 		finalResult =  sum/counter;
	 		memcpy(cursorAggregate, &finalResult, sizeof(int));
	 		this->reachedEndOfFile = true;

	 	 }
	 		 break;

	 	 case SUM :
	 	{
	 		this->sumAndCountAggregrate(&sum, &counter, areAllAtrributesNull);
	 		char * cursorAggregate = (char*) data;
	 		if (areAllAtrributesNull) {
	 			nullbit = true;
	 		}
	 		memcpy(cursorAggregate, &nullbit, sizeof(bool));
	 		cursorAggregate = cursorAggregate + 1;
	 		finalResult = sum;
	 		memcpy(cursorAggregate, &finalResult, sizeof(int));
	 		this->reachedEndOfFile = true;
	 	 }
	 	 break;
	 }
	 free(nextTuple);
	 return 0;

}


RC Aggregate::sumAndCountAggregrate(void *sumData, void *counter, bool &areAllAttributesNullData){
	void *nextTuple = calloc(1,PAGE_SIZE);
	vector <Attribute> attribute;
	this->iterator->getAttributes(attribute);
	float sum = 0;
	float count = 0;
	bool isNull;
	bool areAllAttributesNull = true;
	while(this->iterator->getNextTuple(nextTuple)!=EOF){
		for(int index = 0; index < attribute.size(); index++){
			if(attribute.at(index).name == this->aggAttr.name ){
				 count ++;
					InternalRecord *rf = InternalRecord::parse(attribute,nextTuple,0,false);
						void * attributeData = calloc(1,PAGE_SIZE);
						 rf->getAttributeByIndex(index,attribute,attributeData,isNull);
						 if(!isNull){
							 areAllAttributesNull = false;
							 if(this->aggAttr.type == TypeInt){
								 sum += *((int*)attributeData);
							 }
							 else{
								 sum += *((float*)attributeData);
							 }
						 }
						 free(attributeData);
		}
	}

	}
	memcpy(sumData,&sum,sizeof(int));
	memcpy(counter,&count,sizeof(int));
	areAllAttributesNullData = areAllAttributesNull;
	free(nextTuple);
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const{

		attrs.clear();
	    vector <Attribute> attributes;
	    Attribute aggregrateAttribute;
	    this->iterator->getAttributes(attributes);
	    this->constructAggregrateAttribute(aggregrateAttribute,attributes[this->index]);
	    attrs.push_back(aggregrateAttribute);
}

void Aggregate::constructAggregrateAttribute(Attribute &aggregrateAttribute, Attribute attribute) const{
	string value = EnumStrings[this->oper];
	value.append(attribute.name);
	value.append(")");
	aggregrateAttribute.name = value;

	if(this->oper == MIN || this->oper == MAX ){
		if(attribute.type == TypeInt){
				aggregrateAttribute.type = TypeInt;
			}
		if(attribute.type == TypeReal){
			aggregrateAttribute.type = TypeReal;

		}
		else{
			aggregrateAttribute.type = TypeVarChar;
		}
		aggregrateAttribute.length = attribute.length;
	}
		if(this->oper == COUNT  ){
			aggregrateAttribute.type = TypeReal;
			aggregrateAttribute.length = sizeof(int);
		}
		if(this->oper == AVG){
			aggregrateAttribute.type = TypeReal;
			aggregrateAttribute.length = sizeof(float);
		}
		if(this->oper == SUM){
			aggregrateAttribute.type = TypeReal;
			aggregrateAttribute.length = sizeof(float);
		}

}

RC compareAttributes(void * compAttrValue, void * leftAttribute, Attribute conditionAttribute, CompOp compOp){
		switch (compOp) {
			case EQ_OP: {
			if (compare(leftAttribute, compAttrValue, conditionAttribute, false, false) == 0) {
				return 0;
			}
			break;
		}
		case LE_OP: {
			if (compare(leftAttribute, compAttrValue, conditionAttribute, false, false) >= 0) {
				return 0;
			}
			break;
		}
		case LT_OP: {
			if (compare(leftAttribute, compAttrValue, conditionAttribute, false,false) > 0) {
				return 0;
			}
			break;
		}
		case GT_OP: {
			if (compare(leftAttribute, compAttrValue, conditionAttribute, false,false) < 0) {

				return 0;
			}
			break;
		}
		case GE_OP: {
			if (compare(leftAttribute, compAttrValue, conditionAttribute, false,false) <= 0) {

				return 0;
			}
			break;
		}
		case NE_OP: {
			if (compare(leftAttribute, compAttrValue, conditionAttribute, false,
					false) != 0) {

				return 0;
			}
			break;
		}

		}
		return -1;
}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
		IndexScan *rightIn,          // IndexScan Iterator of input S
		const Condition &condition   // Join condition
		) {
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	vector<Attribute> leftRecordDescriptor;;
	this->leftIn->getAttributes(leftRecordDescriptor);
	this->leftRecord = malloc(PAGE_SIZE);
	this->rightRecord = malloc(PAGE_SIZE);
	this->conditionAttribute = this->rightIn->iter->attr;
	this->leftIn->getNextTuple(this->leftRecord);

	InternalRecord* lir = InternalRecord::parse(leftRecordDescriptor,
						this->leftRecord, 0, false);
	bool isLeftKeyNull;
	void* leftKey = malloc(this->conditionAttribute.length + 4);
	lir->getAttributeValueByName(this->condition.lhsAttr,
						leftRecordDescriptor, leftKey, isLeftKeyNull);
	this->rightIn->setIterator(leftKey, leftKey, true, true);
	freeIfNotNull(leftKey);
};

INLJoin::~INLJoin() {
	freeIfNotNull(this->leftRecord);
	freeIfNotNull(this->rightRecord);
};

RC INLJoin::getNextTuple(void *data) {
	while(true){
		int rc;
		while (this->rightIn->getNextTuple(this->rightRecord) != -1) {
			vector<Attribute> leftRecordDescriptor;
			vector<Attribute> rightRecordDescriptor;
			this->leftIn->getAttributes(leftRecordDescriptor);
			this->rightIn->getAttributes(rightRecordDescriptor);
			InternalRecord* lir = InternalRecord::parse(leftRecordDescriptor,
					this->leftRecord, 0, false);

			InternalRecord* rir = InternalRecord::parse(rightRecordDescriptor,
					this->rightRecord, 0, false);

			void* leftKey = malloc(this->conditionAttribute.length + 4);
			void* rightKey = malloc(this->conditionAttribute.length + 4);
			bool isLeftKeyNull = false;
			bool isRightKeyNull = false;
			rc = lir->getAttributeValueByName(this->condition.lhsAttr,
					leftRecordDescriptor, leftKey, isLeftKeyNull);
			if (rc == -1) {
				freeIfNotNull(leftKey);
				freeIfNotNull(rightKey);
				return -1;
			}
			rc = rir->getAttributeValueByName(this->condition.rhsAttr,
					rightRecordDescriptor, rightKey, isRightKeyNull);
			if (rc == -1) {
				freeIfNotNull(leftKey);
				freeIfNotNull(rightKey);
				return -1;
			}

//
//			RelationManager::instance()->printTuple(leftRecordDescriptor,
//					this->leftRecord);
//
//			RelationManager::instance()->printTuple(rightRecordDescriptor,
//									this->rightRecord);

			if (compareAttributes(rightKey, leftKey, this->conditionAttribute,
					this->condition.op) == 0) {

				vector<Attribute> newRecordDescriptor;
				this->getAttributes(newRecordDescriptor);

				vector<void*> dataArray;
				vector<bool> isNullArray;

				for (int i = 0; i < leftRecordDescriptor.size(); i++) {
					Attribute attr = leftRecordDescriptor[i];
					void* data = malloc(attr.length + 4);
					bool isNull;
					lir->getAttributeByIndex(i, leftRecordDescriptor, data, isNull);
					dataArray.push_back(data);
					isNullArray.push_back(isNull);
				}


				for (int i = 0; i < rightRecordDescriptor.size(); i++) {
					Attribute attr = rightRecordDescriptor[i];
					void* data = malloc(attr.length + 4);
					bool isNull;
					rir->getAttributeByIndex(i, rightRecordDescriptor, data,
							isNull);
					dataArray.push_back(data);
					isNullArray.push_back(isNull);
				}

				mergeAttributesData(newRecordDescriptor, isNullArray, dataArray,
						data);

				for(int i = 0;i<dataArray.size(); i++){
					freeIfNotNull(dataArray[i]);
				}
				freeIfNotNull(leftKey);
				freeIfNotNull(rightKey);
				return 0;
			}
		}
		rc = this->leftIn->getNextTuple(this->leftRecord);
		if(rc == -1) {
	//		left table exhausted
			return -1;
		}

		vector<Attribute> leftRecordDescriptor;;
		this->leftIn->getAttributes(leftRecordDescriptor);
		InternalRecord* lir = InternalRecord::parse(leftRecordDescriptor,
							this->leftRecord, 0, false);
		bool isLeftKeyNull;
		void* leftKey = malloc(this->conditionAttribute.length + 4);
		lir->getAttributeValueByName(this->condition.lhsAttr,
							leftRecordDescriptor, leftKey, isLeftKeyNull);
		this->rightIn->setIterator(leftKey, leftKey, true, true);
		freeIfNotNull(leftKey);
	}
};

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();

    vector<Attribute> temp;
	this->leftIn->getAttributes(temp);
    for(int i=0; i< temp.size(); i++) {
        attrs.push_back(temp[i]);
    }

    temp.clear();
	this->rightIn->getAttributes(temp);
    for(int i=0; i< temp.size(); i++) {
        attrs.push_back(temp[i]);
    }
};

BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
        TableScan *rightIn,           // TableScan Iterator of input S
        const Condition &condition,   // Join condition
        const unsigned numPages       // # of pages that can be loaded into memory,
		                                 //   i.e., memory block size (decided by the optimizer)
 ){
	this->blockSize = numPages*PAGE_SIZE;
	this->condition = condition;
	this->leftIterator = leftIn;
	this->rightIterator = rightIn;
	this->currentBlock = new FixedSizeBlock(this->leftIterator, this->blockSize, this->condition.lhsAttr);
	this->rightRecord = malloc(PAGE_SIZE);
	this->leftRecord = malloc(PAGE_SIZE);
}

BNLJoin::~BNLJoin(){
	freeIfNotNull(this->leftRecord);
	freeIfNotNull(this->rightRecord);
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	vector<Attribute> temp;
	this->leftIterator->getAttributes(temp);
	for(int i=0; i< temp.size(); i++) {
		attrs.push_back(temp[i]);
	}

	temp.clear();
	this->rightIterator->getAttributes(temp);
	for(int i=0; i< temp.size(); i++) {
		attrs.push_back(temp[i]);
	}
}

RC BNLJoin::getNextTuple(void *data) {
	while(true){
		int rc;
		while (this->rightIterator->getNextTuple(this->rightRecord) != -1) {
			vector<Attribute> leftRecordDescriptor;
			vector<Attribute> rightRecordDescriptor;
			this->leftIterator->getAttributes(leftRecordDescriptor);
			this->rightIterator->getAttributes(rightRecordDescriptor);

			InternalRecord* rir = InternalRecord::parse(rightRecordDescriptor,
					this->rightRecord, 0, false);

			void* rightKey = malloc(PAGE_SIZE);
			bool isRightKeyNull = false;

			rc = rir->getAttributeValueByName(this->condition.rhsAttr, rightRecordDescriptor, rightKey, isRightKeyNull);
			if (rc == -1) {
				freeIfNotNull(rightKey);
				return -1;
			}

			rc = this->currentBlock->getByKey(rightKey, this->leftRecord);
			if(rc == -1) {
				freeIfNotNull(rightKey);
				//				not found
				continue;
			}
//			right key found in left buck =>  hence merge them;

			vector<Attribute> newRecordDescriptor;
			this->getAttributes(newRecordDescriptor);

			vector<void*> dataArray;
			vector<bool> isNullArray;

			for (int i = 0; i < leftRecordDescriptor.size(); i++) {
				Attribute attr = leftRecordDescriptor[i];
				void* data = malloc(attr.length + 4);
				bool isNull;
				InternalRecord* lir = InternalRecord::parse(leftRecordDescriptor,
						this->leftRecord, 0, false);
				lir->getAttributeByIndex(i, leftRecordDescriptor, data, isNull);
				dataArray.push_back(data);
				isNullArray.push_back(isNull);
			}


			for (int i = 0; i < rightRecordDescriptor.size(); i++) {
				Attribute attr = rightRecordDescriptor[i];
				void* data = malloc(attr.length + 4);
				bool isNull;
				rir->getAttributeByIndex(i, rightRecordDescriptor, data,
						isNull);
				dataArray.push_back(data);
				isNullArray.push_back(isNull);
			}

			mergeAttributesData(newRecordDescriptor, isNullArray, dataArray,
					data);
			freeIfNotNull(rightKey);

			for(int i=0; i<dataArray.size(); i++){
				freeIfNotNull(dataArray[i]);
			}
			return 0;

		}
		rc = this->currentBlock->getNextBlock();
		if(rc == -1) {
		// left table exhausted
			return -1;
		}
		this->rightIterator->setIterator();
	}
}


Block::Block(Iterator *iterator, string &keyAttributeName) {
	vector<Attribute> recordDescriptor;
	this->iterator = iterator;
	this->count = 0;
	this->iterator->getAttributes(recordDescriptor);
	for(int i=0; i<recordDescriptor.size(); i++) {
		if(recordDescriptor[i].name == keyAttributeName){
			this->keyAttribute = recordDescriptor[i];
			break;
		}
	}
}

Block::~Block() {
}

void Block::setByKey(void* key, void* data) {
	if (this->keyAttribute.type == TypeInt) {
		int keyValue = *(int*) key;
		this->intData[keyValue] = data;
	} else if (this->keyAttribute.type == TypeReal) {
		float keyValue = *(float*) key;
		this->floatData[keyValue] = data;
	} else {
		VarcharParser* vp = new VarcharParser(key);
		string keyValue;
		vp->unParse(keyValue);
		vp->data = NULL;
		delete vp;
		this->stringData[keyValue] = data;
	}
}

RC Block::getByKey(void* key, void* data) {
	vector<Attribute> recordDescriptor;
	this->iterator->getAttributes(recordDescriptor);
	int maxSizePerRecord = InternalRecord::getMaxBytes(recordDescriptor);

	if(this->keyAttribute.type == TypeInt) {
		int keyValue = *(int*)key;
		if(this->intData.find(keyValue) != this->intData.end()){
			memcpy(data, this->intData[keyValue], maxSizePerRecord);
			return 0;
		} else {
			return -1;
		}
	} else if(this->keyAttribute.type == TypeReal) {
		float keyValue = *(float*)key;
		if(this->floatData.find(keyValue) != this->floatData.end()){
			memcpy(data, this->floatData[keyValue], maxSizePerRecord);
			return 0;
		} else {
			return -1;
		}

	} else {
		VarcharParser* vp = new VarcharParser(key);
		string keyValue;
		vp->unParse(keyValue);
		vp->data = NULL;
		delete vp;

		if(this->stringData.find(keyValue) != this->stringData.end()){
			memcpy(data, this->stringData[keyValue], maxSizePerRecord);
			return 0;
		} else {
			return -1;
		}
	}
	return -1;
}

RC Block::getNextBlock() {
//	emptry current block;
//	#TODO garbage collector
	this->clear();

	vector<Attribute> recordDescriptor;
	this->iterator->getAttributes(recordDescriptor);
	int maxSizePerRecord = InternalRecord::getMaxBytes(recordDescriptor);
	int maxSizePerKey = this->keyAttribute.length+sizeof(int);
	void* data = malloc(maxSizePerRecord);
	count = 1;
	while(!this->shouldStopInserting()) {
		if(this->iterator->getNextTuple(data) == -1){
			if(count == 1) {
				return -1;
			} else {
				return 0;
			}
		}
		InternalRecord* ir = InternalRecord::parse(recordDescriptor, data, 0, false);
		void* key = malloc(maxSizePerKey);
		bool isNull;
		ir->getAttributeValueByName(this->keyAttribute.name, recordDescriptor, key, isNull);
		this->setByKey(key, data);
		data = malloc(maxSizePerRecord);
		key = malloc(maxSizePerKey);
		this->count++;
	}
	return 0;
}

GHJoin::GHJoin(Iterator* leftIn, Iterator* rightIn, const Condition& condition, unsigned int numPartions){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPartitions = numPartions;
	time (&(this->joinTimeStamp));
	vector<Attribute> leftAttributes;
	vector<Attribute> rightAttributes;
	this->leftIn->getAttributes(leftAttributes);
	this->rightIn->getAttributes(rightAttributes);
	this->leftRecord = malloc(PAGE_SIZE);
	this->rightRecord = malloc(PAGE_SIZE);
	this->createPartitions(this->condition.lhsAttr, this->leftIn, this->condition.lhsAttr, numPartitions);
	this->createPartitions(this->condition.rhsAttr, this->rightIn, this->condition.rhsAttr, numPartitions);
	this->partitionLeftIn = new PartitionIterator(this->condition.lhsAttr, numPartitions, leftAttributes);
	this->partitionRightIn = new PartitionIterator(this->condition.rhsAttr, numPartitions, rightAttributes);
	this->partitionLeftIn->isFullTableScanMode = false;
	this->partitionRightIn->isFullTableScanMode = false;
	this->currentBlock = new PartitionBlock(this->partitionLeftIn, this->condition.lhsAttr);
}

int GHJoin::getPartitionIndexByKey(void* key, Attribute& attr){
	if(attr.type == TypeInt) {
		int keyValue = *((int*)key);
		return keyValue%numPartitions;
	} else if (attr.type == TypeReal) {
		float keyValue = *((float*)key);
		return ((int)keyValue)%numPartitions;
	} else {
		VarcharParser* vp = new VarcharParser(key);
		string keyValue;
		vp->unParse(keyValue);
		vp->data = NULL;
		delete vp;
		int hash = 7;
		for (int i = 0; i < keyValue.size(); i++) {
		    hash = hash*31 + (int)keyValue[i];
		}
		return hash%numPartitions;
	}
}

GHJoin::~GHJoin() {
	RelationManager* rm = RelationManager::instance();
	for(int i=0; i<this->numPartitions; i++){
		string tableName = this->partitionLeftIn->relationName + '_' + to_string(i);
		rm->deleteTable(tableName);
		rm->deleteTable(this->partitionRightIn->relationName + '_' + to_string(i));
	}
}

RC GHJoin::getNextTuple(void* data) {
	while(true){
		while(this->partitionRightIn->getNextTuple(this->rightRecord) != -1){
				vector<Attribute> rightRecordDescriptor;
				this->rightIn->getAttributes(rightRecordDescriptor);
				InternalRecord* rir = InternalRecord::parse(rightRecordDescriptor, this->rightRecord, 0, false);
				void* rightKey = malloc(rir->getMaxBytes(rightRecordDescriptor));
				bool isNull;
				rir->getAttributeValueByName(this->condition.rhsAttr, rightRecordDescriptor, rightKey, isNull);
				if(this->currentBlock->getByKey(rightKey, this->leftRecord)!= -1){
					// it is is hit
					vector<Attribute> leftAttrs;
					this->leftIn->getAttributes(leftAttrs);
					InternalRecord * lir = InternalRecord::parse(leftAttrs, this->leftRecord, 0, false);

					vector<Attribute> newRecordDescriptor;
					this->getAttributes(newRecordDescriptor);

					vector<void*> dataArray;
					vector<bool> isNullArray;

					for (int i = 0; i < leftAttrs.size(); i++) {
						Attribute attr = leftAttrs[i];
						void* data = malloc(attr.length + 4);
						bool isNull;
						InternalRecord* lir = InternalRecord::parse(leftAttrs,
								this->leftRecord, 0, false);
						lir->getAttributeByIndex(i, leftAttrs, data, isNull);
						dataArray.push_back(data);
						isNullArray.push_back(isNull);
					}


					for (int i = 0; i < rightRecordDescriptor.size(); i++) {
						Attribute attr = rightRecordDescriptor[i];
						void* data = malloc(attr.length + 4);
						bool isNull;
						rir->getAttributeByIndex(i, rightRecordDescriptor, data,
								isNull);
						dataArray.push_back(data);
						isNullArray.push_back(isNull);
					}

//					RelationManager::instance()->printTuple(leftAttrs, this->leftRecord);
//					RelationManager::instance()->printTuple(rightRecordDescriptor, this->rightRecord);
					mergeAttributesData(newRecordDescriptor, isNullArray, dataArray, data);
					return 0;
				}
			}
            if(this->partitionLeftIn->next() != -1){
				this->currentBlock->getNextBlock();
				this->partitionRightIn->next();
			} else {
				return -1;
			}
	}
}

TableScan* PartitionIterator::getTableScanIteratorByIndex(int index) {
	if(index < numPartitions) {
		string name = this->relationName + "_" + to_string(index);
		RelationManager* rm = RelationManager::instance();
		return new TableScan(*rm, name);
	}
	return NULL;
}

PartitionIterator::PartitionIterator(string relationName, unsigned int numPartitions, vector<Attribute> &attrs){
	this->relationName  = relationName;
	this->numPartitions = numPartitions;
	this->attrs = attrs;
	isFullTableScanMode = true;
	currentPartitionIndex = 0;
	ts = getTableScanIteratorByIndex(currentPartitionIndex);
}

RC PartitionIterator::next(){
	if(currentPartitionIndex < numPartitions-1){
		currentPartitionIndex += 1;
		this->ts = getTableScanIteratorByIndex(currentPartitionIndex);
		return 0;
	}
	return -1;
}

RC PartitionIterator::getNextTuple(void* data) {
	while(true){
		if(ts == NULL) {
			return -1;
		}
		if(ts->getNextTuple(data)!= -1){
			return 0;
		} else {
			if(isFullTableScanMode){
				currentPartitionIndex += 1;
				ts = getTableScanIteratorByIndex(currentPartitionIndex);
			} else {
				return -1;
			}
		}
	}
}

PartitionIterator::~PartitionIterator(){
//	#TODO fix memory leaks
}

void PartitionIterator::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	for(int i=0; i< this->attrs.size(); i++) {
		attrs.push_back(this->attrs[i]);
	}
}

void GHJoin::createPartitions(string relationName, Iterator* iter, string &attributeName, unsigned int numPartitions) {
	RelationManager* rm = RelationManager::instance();
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	FileHandle fh;
	vector<Attribute> recordDescriptor;
	iter->getAttributes(recordDescriptor);
	Attribute attr;
	for(int i=0; i<recordDescriptor.size(); i++) {
		attr =recordDescriptor[i];
		if(attr.name == attributeName) {
			break;
		}
	}

	for(int i=0; i<numPartitions; i++) {
		string partitionName = relationName + "_" + to_string(i);
		rm->createTable(partitionName, recordDescriptor);
	}
	void* data = malloc(InternalRecord::getMaxBytes(recordDescriptor));
	void* key = malloc(attr.length + sizeof(int));
	RID rid;
	int count = 0;
	while(iter->getNextTuple(data)!= -1) {
		InternalRecord* ir = InternalRecord::parse(recordDescriptor, data, 0, false);
		bool isNull;
		ir->getAttributeValueByName(attributeName, recordDescriptor, key, isNull);
		int index = this->getPartitionIndexByKey(key, attr);
		rbfm->openFile(relationName + "_" + to_string(index), fh);
		rbfm->insertRecord(fh, recordDescriptor, data, rid);
		count++;
	}
	rbfm->closeFile(fh);
}


void GHJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	vector<Attribute> temp;
	this->leftIn->getAttributes(temp);
	for(int i=0; i< temp.size(); i++) {
		attrs.push_back(temp[i]);
	}

	temp.clear();
	this->rightIn->getAttributes(temp);
	for(int i=0; i< temp.size(); i++) {
		attrs.push_back(temp[i]);
	}
}

void PartitionIterator::reset(){
	this->currentPartitionIndex = 0;
}
