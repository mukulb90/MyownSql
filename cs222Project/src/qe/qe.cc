
#include "qe.h"

Filter::Filter(Iterator *input, const Condition & condition ){
	this->iterator = input;
	this->condition = condition;
}

//Filter::~Filter(){
//	this->iterator = 0;
//}

static int count_var = 0;

RC Filter::getNextTuple(void* data) {
	while(iterator->getNextTuple(data) != EOF) {
		if(compareCondition(data) == 0) {
			return 0;
		}
	}
	return -1;

}

void Filter::getAttributes(vector<Attribute> &attrs) const {
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
						comparisonResult = compareAttributes(rightAttribute,leftAttribute, attr.at(index),this->condition.op);
						return comparisonResult;
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
	bool isNull;
	char* cursor = (char*) data;
	vector <Attribute> attr;
	this->iterator->getAttributes(attr);
	void *nextTuple = calloc(1,PAGE_SIZE);
	while(this->iterator->getNextTuple(nextTuple)!=EOF){
		for(int i = 0; i<projectedAttr.size(); i++){
			for(int index=0; index< attr.size(); index++){
				if(attr.at(index).name == projectedAttr.at(i).name){
					InternalRecord *ir = InternalRecord::parse(attr,nextTuple,0,false);
					void *projectedData = calloc(1,PAGE_SIZE);
					ir->getAttributeByIndex(index, attr, projectedData,isNull);
						if(projectedAttr.at(i).type == TypeVarChar){
							int length = 0;
							memcpy(&length, projectedData, sizeof(int));
							memcpy(cursor, projectedData, length+sizeof(int));
							cursor = cursor + length+sizeof(int);
							freeIfNotNull(projectedData);
						}
						else{
							memcpy(cursor,projectedData, sizeof(int));
							cursor = cursor+sizeof(int);
							freeIfNotNull(projectedData);

						}


					}
				}
			}

		return 0;
		}
	return -1;
	}

void Project::getAttributes(vector<Attribute> &attrs)const{
	this->iterator->getAttributes(attrs);
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

